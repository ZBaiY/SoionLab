from __future__ import annotations

import asyncio
import copy
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean, median
from typing import Any

from analyze.backtest.reporter import generate_backtest_artifacts
from apps.run_code.backtest_app import run_backtest_app
from quant_engine.strategy.registry import get_strategy
from quant_engine.utils.paths import data_root_from_file


REPO_ROOT = Path(__file__).resolve().parents[4]
ROOT = REPO_ROOT / "docs/strategies/rsi_adxgateway"
DATA_ROOT = data_root_from_file("apps/run_backtest.py", levels_up=1)
STRATEGY_NAME = "RSI-ADX-SIDEWAYS-FRACTIONAL"
BASE_BIND = {"A": "BTCUSDT", "window_RSI": "14", "window_ADX": "14", "window_RSI_rolling": "5"}
OUT_JSON = ROOT / "analysis_results.json"
OUT_CSV = ROOT / "tuning_grid.csv"
OUT_MD = ROOT / "multi_dimension_analysis.md"


TUNING_WINDOWS = [
    ("2024-02-01", 1706745600000, 1709251200000),
    ("2024-07-01", 1719792000000, 1722470400000),
    ("2025-03-01", 1740787200000, 1743465600000),
    ("2025-07-01", 1751328000000, 1754006400000),
    ("2025-11-01", 1761955200000, 1764547200000),
]

# Exclude 2024-11 because its trace artifact wrapped to the start timestamp mid-file.
ROBUSTNESS_WINDOWS = [
    ("2024-02-01/2024-04-01", 1706745600000, 1711929600000),
    ("2024-04-01/2024-06-01", 1711929600000, 1717200000000),
    ("2024-06-01/2024-08-01", 1717200000000, 1722470400000),
    ("2024-08-01/2024-10-01", 1722470400000, 1727740800000),
    ("2024-12-01/2025-02-01", 1733011200000, 1738368000000),
    ("2025-02-01/2025-04-01", 1738368000000, 1743465600000),
    ("2025-04-01/2025-06-01", 1743465600000, 1748736000000),
    ("2025-06-01/2025-08-01", 1748736000000, 1754006400000),
    ("2025-08-01/2025-10-01", 1754006400000, 1759276800000),
    ("2025-10-01/2025-12-01", 1759276800000, 1764547200000),
    ("2025-12-01/2026-02-01", 1764547200000, 1769904000000),
]

ADX_THRESHOLDS = [20, 25, 30]
VARIANCE_FACTORS = [1.4, 1.8, 2.2]
ROLLING_WINDOWS = [3, 5, 7]


@dataclass
class RunMetric:
    label: str
    start_ts: int
    end_ts: int
    sharpe: float
    total_return: float
    total_trades: int
    max_drawdown: float
    quality_verdict: str


def _decision_cfg(*, adx_threshold: int, variance_factor: float) -> dict[str, Any]:
    return {
        "type": "RSI-DYNAMIC-BAND",
        "params": {
            "rsi": "RSI_DECISION_{A}",
            "rsi_mean": "RSI-MEAN_DECISION_{A}",
            "rsi_std": "RSI-STD_DECISION_{A}",
            "adx": "ADX_DECISION_{A}",
            "adx_threshold": adx_threshold,
            "variance_factor": variance_factor,
            "mae": 0.0,
        },
    }


async def _run_once(
    *,
    run_id: str,
    bind_symbols: dict[str, str],
    adx_threshold: int,
    variance_factor: float,
    start_ts: int,
    end_ts: int,
) -> RunMetric:
    strategy = get_strategy(STRATEGY_NAME)
    orig_decision = copy.deepcopy(strategy.DECISION_CFG)
    try:
        strategy.DECISION_CFG = _decision_cfg(
            adx_threshold=adx_threshold,
            variance_factor=variance_factor,
        )
        await run_backtest_app(
            strategy_name=STRATEGY_NAME,
            bind_symbols=bind_symbols,
            start_ts=start_ts,
            end_ts=end_ts,
            data_root=DATA_ROOT,
            run_id=run_id,
        )
    finally:
        strategy.DECISION_CFG = orig_decision

    summary = json.loads((Path("artifacts/runs") / run_id / "report/summary.json").read_text())
    perf = dict(summary.get("performance_summary") or {})
    return RunMetric(
        label=run_id,
        start_ts=int(summary["start_ts_ms"]),
        end_ts=int(summary["end_ts_ms"]),
        sharpe=float(perf.get("sharpe") or 0.0),
        total_return=float(perf.get("total_return") or 0.0),
        total_trades=int(perf.get("total_trades") or 0),
        max_drawdown=float(perf.get("max_drawdown") or 0.0),
        quality_verdict=str(summary.get("quality_verdict") or "UNKNOWN"),
    )


def _score_combo(rows: list[RunMetric]) -> dict[str, float]:
    clean = [r for r in rows if r.quality_verdict == "CLEAN"]
    if not clean:
        return {
            "clean_ratio": 0.0,
            "median_sharpe": -999.0,
            "mean_return": -999.0,
            "median_trades": 0.0,
            "objective": -999.0,
        }
    median_sharpe = float(median(r.sharpe for r in clean))
    mean_return = float(mean(r.total_return for r in clean))
    median_trades = float(median(r.total_trades for r in clean))
    clean_ratio = len(clean) / len(rows)
    # Emphasize median Sharpe, use return and trade activity as tie-breakers.
    objective = median_sharpe + 0.35 * mean_return + 0.002 * median_trades + 0.25 * clean_ratio
    return {
        "clean_ratio": clean_ratio,
        "median_sharpe": median_sharpe,
        "mean_return": mean_return,
        "median_trades": median_trades,
        "objective": objective,
    }


async def main() -> None:
    tuning_grid: list[dict[str, Any]] = []
    best_combo: dict[str, Any] | None = None
    combo_runs: dict[tuple[int, float, int], list[RunMetric]] = {}

    for adx_threshold in ADX_THRESHOLDS:
        for variance_factor in VARIANCE_FACTORS:
            for rolling_window in ROLLING_WINDOWS:
                bind = dict(BASE_BIND)
                bind["window_RSI_rolling"] = str(rolling_window)
                rows: list[RunMetric] = []
                for label, start_ts, end_ts in TUNING_WINDOWS:
                    run_id = (
                        f"TUNE_RSI_ADX_A{adx_threshold}_V{str(variance_factor).replace('.', 'p')}"
                        f"_R{rolling_window}_{label.replace('-', '')}"
                    )
                    metric = await _run_once(
                        run_id=run_id,
                        bind_symbols=bind,
                        adx_threshold=adx_threshold,
                        variance_factor=variance_factor,
                        start_ts=start_ts,
                        end_ts=end_ts,
                    )
                    metric.label = label
                    rows.append(metric)
                combo_runs[(adx_threshold, variance_factor, rolling_window)] = rows
                score = _score_combo(rows)
                result = {
                    "adx_threshold": adx_threshold,
                    "variance_factor": variance_factor,
                    "window_RSI_rolling": rolling_window,
                    **score,
                }
                tuning_grid.append(result)
                if best_combo is None or result["objective"] > best_combo["objective"]:
                    best_combo = result

    assert best_combo is not None
    tuned_key = (
        int(best_combo["adx_threshold"]),
        float(best_combo["variance_factor"]),
        int(best_combo["window_RSI_rolling"]),
    )
    tuned_bind = dict(BASE_BIND)
    tuned_bind["window_RSI_rolling"] = str(tuned_key[2])

    robustness_rows: list[dict[str, Any]] = []
    for label, start_ts, end_ts in ROBUSTNESS_WINDOWS:
        run_id = (
            f"ROBUST_TUNED_A{tuned_key[0]}_V{str(tuned_key[1]).replace('.', 'p')}"
            f"_R{tuned_key[2]}_{label.split('/')[0].replace('-', '')}"
        )
        metric = await _run_once(
            run_id=run_id,
            bind_symbols=tuned_bind,
            adx_threshold=tuned_key[0],
            variance_factor=tuned_key[1],
            start_ts=start_ts,
            end_ts=end_ts,
        )
        robustness_rows.append(asdict(metric) | {"window": label})

    sensitivity: dict[str, list[dict[str, Any]]] = {"adx_threshold": [], "variance_factor": [], "window_RSI_rolling": []}
    for adx_threshold in ADX_THRESHOLDS:
        score = _score_combo(combo_runs[(adx_threshold, tuned_key[1], tuned_key[2])])
        sensitivity["adx_threshold"].append({"value": adx_threshold, **score})
    for variance_factor in VARIANCE_FACTORS:
        score = _score_combo(combo_runs[(tuned_key[0], variance_factor, tuned_key[2])])
        sensitivity["variance_factor"].append({"value": variance_factor, **score})
    for rolling_window in ROLLING_WINDOWS:
        score = _score_combo(combo_runs[(tuned_key[0], tuned_key[1], rolling_window)])
        sensitivity["window_RSI_rolling"].append({"value": rolling_window, **score})

    payload = {
        "method": {
            "strategy": STRATEGY_NAME,
            "fixed_bind_symbols": BASE_BIND,
            "tuning_windows": TUNING_WINDOWS,
            "robustness_windows": ROBUSTNESS_WINDOWS,
            "objective": "maximize median_sharpe with mean_return, median_trades, clean_ratio tie-breakers",
        },
        "best_combo": best_combo,
        "tuning_grid": tuning_grid,
        "robustness_rows": robustness_rows,
        "sensitivity": sensitivity,
        "excluded_window_note": (
            "2024-11-01 to 2024-12-01 was excluded from tuning because its trace wrapped back to the "
            "run start mid-file, producing duplicate timestamps and one non-monotonic transition."
        ),
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with OUT_CSV.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=[
                "adx_threshold",
                "variance_factor",
                "window_RSI_rolling",
                "clean_ratio",
                "median_sharpe",
                "mean_return",
                "median_trades",
                "objective",
            ],
        )
        writer.writeheader()
        writer.writerows(tuning_grid)

    robust_clean = [r for r in robustness_rows if r["quality_verdict"] == "CLEAN"]
    md = []
    md.append("# Dynamical RSI + ADX Gateway Multi-Dimension Analysis")
    md.append("")
    md.append("## Scope")
    md.append("")
    md.append(
        "This note tunes `RSI-ADX-SIDEWAYS-FRACTIONAL` on clean monthly BTCUSDT windows and then checks the tuned set "
        "on wider rolling 60-day windows."
    )
    md.append("")
    md.append("Fixed bindings:")
    md.append("")
    md.append("- `A=BTCUSDT`")
    md.append("- `window_RSI=14`")
    md.append("- `window_ADX=14`")
    md.append("- strategy: `RSI-ADX-SIDEWAYS-FRACTIONAL`")
    md.append("")
    md.append("Tuned dimensions:")
    md.append("")
    md.append(f"- `adx_threshold`: {ADX_THRESHOLDS}")
    md.append(f"- `variance_factor`: {VARIANCE_FACTORS}")
    md.append(f"- `window_RSI_rolling`: {ROLLING_WINDOWS}")
    md.append("")
    md.append("## Invalid Window Resolution")
    md.append("")
    md.append(
        "The earlier `2024-11-01` to `2024-12-01` run was marked `INVALID` because its trace file wrapped from "
        "`1732465800000` back to `1730419200000` mid-file. That is a trace artifact, not evidence that this strategy "
        "failed economically on that window. I excluded that window from the tuning objective."
    )
    md.append("")
    md.append("## Best Parameter Set")
    md.append("")
    md.append(f"- `adx_threshold = {best_combo['adx_threshold']}`")
    md.append(f"- `variance_factor = {best_combo['variance_factor']}`")
    md.append(f"- `window_RSI_rolling = {best_combo['window_RSI_rolling']}`")
    md.append(f"- tuning objective = {best_combo['objective']:.4f}")
    md.append(f"- median Sharpe across clean tuning windows = {best_combo['median_sharpe']:.4f}")
    md.append(f"- mean return across clean tuning windows = {best_combo['mean_return']:.4%}")
    md.append(f"- median trades across clean tuning windows = {best_combo['median_trades']:.1f}")
    md.append("")
    md.append("## Top 5 Tuning Rows")
    md.append("")
    md.append("| adx_threshold | variance_factor | window_RSI_rolling | median_sharpe | mean_return | median_trades | objective |")
    md.append("|---:|---:|---:|---:|---:|---:|---:|")
    for row in sorted(tuning_grid, key=lambda x: x["objective"], reverse=True)[:5]:
        md.append(
            f"| {row['adx_threshold']} | {row['variance_factor']} | {row['window_RSI_rolling']} | "
            f"{row['median_sharpe']:.4f} | {row['mean_return']:.4%} | {row['median_trades']:.1f} | {row['objective']:.4f} |"
        )
    md.append("")
    md.append("## 60-Day Rolling Robustness")
    md.append("")
    md.append("| window | sharpe | total_return | total_trades | max_drawdown | verdict |")
    md.append("|---|---:|---:|---:|---:|---|")
    for row in robustness_rows:
        md.append(
            f"| {row['window']} | {row['sharpe']:.4f} | {row['total_return']:.4%} | {row['total_trades']} | "
            f"{row['max_drawdown']:.4%} | {row['quality_verdict']} |"
        )
    md.append("")
    if robust_clean:
        md.append("Aggregate over clean 60-day windows:")
        md.append("")
        md.append(f"- mean Sharpe = {mean(r['sharpe'] for r in robust_clean):.4f}")
        md.append(f"- median Sharpe = {median(r['sharpe'] for r in robust_clean):.4f}")
        md.append(f"- mean return = {mean(r['total_return'] for r in robust_clean):.4%}")
        md.append(f"- positive-return window ratio = {sum(1 for r in robust_clean if r['total_return'] > 0) / len(robust_clean):.2%}")
        md.append(f"- median trades = {median(r['total_trades'] for r in robust_clean):.1f}")
    md.append("")
    md.append("## Parameterwise Robustness")
    md.append("")
    for name, rows in sensitivity.items(): # type: ignore # types: ignore
        md.append(f"### {name}")
        md.append("")
        md.append("| value | median_sharpe | mean_return | median_trades | objective |")
        md.append("|---:|---:|---:|---:|---:|")
        for row in rows:
            md.append(
                f"| {row['value']} | {row['median_sharpe']:.4f} | {row['mean_return']:.4%} | " # type: ignore
                f"{row['median_trades']:.1f} | {row['objective']:.4f} |" # type: ignore
            )
        md.append("")
    md.append("## Interpretation")
    md.append("")
    md.append(
        "The tuned set should be treated as the best configuration inside this narrow local grid, not as a globally "
        "optimal parameter set. The parameterwise tables show whether the tuned point sits inside a broad plateau or a fragile spike."
    )
    md.append("")
    md.append("Source files:")
    md.append("")
    md.append(f"- `{OUT_JSON.name}`")
    md.append(f"- `{OUT_CSV.name}`")
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(main())
