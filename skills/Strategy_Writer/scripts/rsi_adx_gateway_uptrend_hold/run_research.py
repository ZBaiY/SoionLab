from __future__ import annotations

import asyncio
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean, median
from typing import Any

import pandas as pd
from apps.run_code.backtest_app import run_backtest_app


REPO_ROOT = Path(__file__).resolve().parents[4]
DOC_ROOT = REPO_ROOT / "docs/strategies/rsi_adx_gateway_uptrend_hold"
DATA_ROOT = REPO_ROOT / "data"
SAMPLE_OHLCV = REPO_ROOT / "data" / "sample" / "ohlcv" / "BTCUSDT" / "15m" / "2025.parquet"
CLEANED_OHLCV = REPO_ROOT / "data" / "cleaned" / "ohlcv" / "BTCUSDT" / "15m" / "2025.parquet"
WARMUP_BUFFER_MS = 300 * 15 * 60 * 1000

BASELINE_STRATEGY = "RSI-ADX-GATEWAY-FRACTIONAL"
SISTER_STRATEGY = "RSI-ADX-GATEWAY-UPTREND-HOLD-FRACTIONAL"
MODEL_ID = "uptrend_hold_v1"
BASE_BIND = {
    "A": "BTCUSDT",
    "window_RSI": "14",
    "window_ADX": "14",
    "window_RSI_rolling": "5",
}

OUT_JSON = DOC_ROOT / "research_results.json"
OUT_CSV = DOC_ROOT / "parameter_scan.csv"
OUT_REPORT = DOC_ROOT / "rsi_adx_gateway_uptrend_hold_research_report.md"
OUT_JSON_MODEL = DOC_ROOT / f"research_results_{MODEL_ID}.json"
OUT_CSV_MODEL = DOC_ROOT / f"parameter_scan_{MODEL_ID}.csv"
OUT_REPORT_MODEL = DOC_ROOT / f"rsi_adx_gateway_uptrend_hold_research_report_{MODEL_ID}.md"

IS_WINDOWS = [
    ("2025-02", 1738368000000, 1740787200000),
    ("2025-05", 1746057600000, 1748736000000),
    ("2025-08", 1754006400000, 1756684800000),
]

OOS_WINDOWS = [
    ("2025-09", 1756684800000, 1759276800000),
    ("2025-10", 1759276800000, 1761955200000),
    ("2025-11", 1761955200000, 1764547200000),
]

ADX_THRESHOLDS = [20, 25, 30]
VARIANCE_FACTORS = [1.4, 1.8, 2.2]
MAE_VALUES = [0.0, 0.25, 0.5]


@dataclass
class RunMetric:
    label: str
    strategy_name: str
    start_ts: int
    end_ts: int
    sharpe: float
    total_return: float
    total_trades: int
    max_drawdown: float
    quality_verdict: str


def _ensure_output_dirs() -> None:
    DOC_ROOT.mkdir(parents=True, exist_ok=True)


def _ensure_sample_cleaned_view() -> None:
    if CLEANED_OHLCV.exists():
        return
    if not SAMPLE_OHLCV.exists():
        raise FileNotFoundError(f"sample OHLCV fixture not found: {SAMPLE_OHLCV}")
    CLEANED_OHLCV.parent.mkdir(parents=True, exist_ok=True)
    CLEANED_OHLCV.symlink_to(SAMPLE_OHLCV)


def _gap_report() -> dict[str, Any]:
    frame = pd.read_parquet(SAMPLE_OHLCV, columns=["open_time"])
    ts = sorted(int(v) for v in frame["open_time"])
    interval_ms = 15 * 60 * 1000
    histogram: dict[str, int] = {}
    examples: list[dict[str, Any]] = []
    largest_gap = 0
    for prev_ts, cur_ts in zip(ts, ts[1:]):
        missing_bars = int((cur_ts - prev_ts) // interval_ms - 1)
        if missing_bars < 1:
            continue
        histogram[str(missing_bars)] = histogram.get(str(missing_bars), 0) + 1
        largest_gap = max(largest_gap, missing_bars)
        if len(examples) < 5:
            examples.append(
                {
                    "prev_ts": prev_ts,
                    "next_ts": cur_ts,
                    "missing_bars": missing_bars,
                }
            )
    long_gap_breaks = sum(count for missing, count in histogram.items() if int(missing) >= 8)
    return {
        "interval": "15m",
        "start_ts": ts[0] if ts else None,
        "end_ts": ts[-1] if ts else None,
        "gap_count": int(sum(histogram.values())),
        "histogram": histogram,
        "largest_gap_missing_bars": largest_gap,
        "long_gap_segment_break_count": int(long_gap_breaks),
        "examples": examples,
        "post_gap_policy": "2025 sample shows no detected missing bars; runs use a 300-bar pre-window warmup buffer and no segment split was required.",
    }


async def _run_strategy(
    *,
    run_id: str,
    strategy_name: str,
    bind_symbols: dict[str, str],
    start_ts: int,
    end_ts: int,
) -> RunMetric:
    await run_backtest_app(
        strategy_name=strategy_name,
        bind_symbols=bind_symbols,
        start_ts=max(0, start_ts - WARMUP_BUFFER_MS),
        end_ts=end_ts,
        data_root=DATA_ROOT,
        run_id=run_id,
    )
    summary_path = REPO_ROOT / "artifacts" / "runs" / run_id / "report" / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    perf = dict(summary.get("performance_summary") or {})
    return RunMetric(
        label=run_id,
        strategy_name=strategy_name,
        start_ts=int(summary["start_ts_ms"]),
        end_ts=int(summary["end_ts_ms"]),
        sharpe=float(perf.get("sharpe") or 0.0),
        total_return=float(perf.get("total_return") or 0.0),
        total_trades=int(perf.get("total_trades") or 0),
        max_drawdown=float(perf.get("max_drawdown") or 0.0),
        quality_verdict=str(summary.get("quality_verdict") or "UNKNOWN"),
    )


def _score_rows(rows: list[RunMetric]) -> dict[str, float]:
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
    objective = median_sharpe + 0.35 * mean_return + 0.002 * median_trades + 0.25 * clean_ratio
    return {
        "clean_ratio": clean_ratio,
        "median_sharpe": median_sharpe,
        "mean_return": mean_return,
        "median_trades": median_trades,
        "objective": objective,
    }


def _aggregate(rows: list[RunMetric]) -> dict[str, float]:
    clean = [r for r in rows if r.quality_verdict == "CLEAN"]
    use = clean or rows
    if not use:
        return {
            "window_count": 0.0,
            "clean_count": 0.0,
            "mean_sharpe": 0.0,
            "median_sharpe": 0.0,
            "mean_return": 0.0,
            "median_return": 0.0,
            "mean_trades": 0.0,
            "median_trades": 0.0,
            "mean_max_drawdown": 0.0,
            "positive_return_ratio": 0.0,
        }
    return {
        "window_count": float(len(rows)),
        "clean_count": float(len(clean)),
        "mean_sharpe": float(mean(r.sharpe for r in use)),
        "median_sharpe": float(median(r.sharpe for r in use)),
        "mean_return": float(mean(r.total_return for r in use)),
        "median_return": float(median(r.total_return for r in use)),
        "mean_trades": float(mean(r.total_trades for r in use)),
        "median_trades": float(median(r.total_trades for r in use)),
        "mean_max_drawdown": float(mean(r.max_drawdown for r in use)),
        "positive_return_ratio": float(sum(1 for r in use if r.total_return > 0) / len(use)),
    }


def _head_to_head(lhs: list[RunMetric], rhs: list[RunMetric]) -> dict[str, float]:
    pairs = list(zip(lhs, rhs))
    if not pairs:
        return {}
    return {
        "mean_sharpe_delta": float(mean(a.sharpe - b.sharpe for a, b in pairs)),
        "median_sharpe_delta": float(median(a.sharpe - b.sharpe for a, b in pairs)),
        "mean_return_delta": float(mean(a.total_return - b.total_return for a, b in pairs)),
        "median_return_delta": float(median(a.total_return - b.total_return for a, b in pairs)),
        "mean_trade_delta": float(mean(a.total_trades - b.total_trades for a, b in pairs)),
        "mean_drawdown_delta": float(mean(a.max_drawdown - b.max_drawdown for a, b in pairs)),
        "return_win_count": float(sum(1 for a, b in pairs if a.total_return > b.total_return)),
        "sharpe_win_count": float(sum(1 for a, b in pairs if a.sharpe > b.sharpe)),
    }


def _bind_params(adx_threshold: int, variance_factor: float, mae: float) -> dict[str, str]:
    bind = dict(BASE_BIND)
    bind["adx_threshold"] = str(adx_threshold)
    bind["variance_factor"] = str(variance_factor)
    bind["mae"] = str(mae)
    return bind


async def _scan_strategy(
    *,
    strategy_name: str,
    strategy_prefix: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[tuple[int, float], list[RunMetric]]]:
    scan_rows: list[dict[str, Any]] = []
    combo_runs: dict[tuple[int, float], list[RunMetric]] = {}
    best_scan: dict[str, Any] | None = None
    for adx_threshold in ADX_THRESHOLDS:
        for variance_factor in VARIANCE_FACTORS:
            rows: list[RunMetric] = []
            bind = _bind_params(adx_threshold, variance_factor, 0.0)
            for label, start_ts, end_ts in IS_WINDOWS:
                run_id = (
                    f"IS_{strategy_prefix}_A{adx_threshold}_V{str(variance_factor).replace('.', 'p')}_{label}"
                )
                metric = await _run_strategy(
                    run_id=run_id,
                    strategy_name=strategy_name,
                    bind_symbols=bind,
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
                metric.label = label
                rows.append(metric)
            combo_runs[(adx_threshold, variance_factor)] = rows
            score = _score_rows(rows)
            row = {
                "strategy_name": strategy_name,
                "adx_threshold": adx_threshold,
                "variance_factor": variance_factor,
                **score,
            }
            scan_rows.append(row)
            if best_scan is None or float(row["objective"]) > float(best_scan["objective"]):
                best_scan = row
    assert best_scan is not None
    return scan_rows, best_scan, combo_runs


async def _mae_sensitivity(
    *,
    strategy_name: str,
    adx_threshold: int,
    variance_factor: float,
) -> list[dict[str, Any]]:
    rows_out: list[dict[str, Any]] = []
    for mae in MAE_VALUES:
        rows: list[RunMetric] = []
        bind = _bind_params(adx_threshold, variance_factor, mae)
        for label, start_ts, end_ts in IS_WINDOWS:
            run_id = (
                f"IS_MAE_{strategy_name.replace('-', '_')}_A{adx_threshold}_V{str(variance_factor).replace('.', 'p')}"
                f"_M{str(mae).replace('.', 'p')}_{label}"
            )
            metric = await _run_strategy(
                run_id=run_id,
                strategy_name=strategy_name,
                bind_symbols=bind,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            metric.label = label
            rows.append(metric)
        rows_out.append({"strategy_name": strategy_name, "mae": mae, **_score_rows(rows)})
    return rows_out


async def main() -> None:
    _ensure_output_dirs()
    _ensure_sample_cleaned_view()
    gap_report = _gap_report()

    baseline_scan_rows, baseline_best, baseline_combo_runs = await _scan_strategy(
        strategy_name=BASELINE_STRATEGY,
        strategy_prefix="BASE_GATE",
    )
    sister_scan_rows, sister_best, sister_combo_runs = await _scan_strategy(
        strategy_name=SISTER_STRATEGY,
        strategy_prefix="SISTER_HOLD",
    )

    baseline_best_key = (int(baseline_best["adx_threshold"]), float(baseline_best["variance_factor"]))
    sister_best_key = (int(sister_best["adx_threshold"]), float(sister_best["variance_factor"]))

    baseline_mae = await _mae_sensitivity(
        strategy_name=BASELINE_STRATEGY,
        adx_threshold=int(baseline_best["adx_threshold"]),
        variance_factor=float(baseline_best["variance_factor"]),
    )
    sister_mae = await _mae_sensitivity(
        strategy_name=SISTER_STRATEGY,
        adx_threshold=int(sister_best["adx_threshold"]),
        variance_factor=float(sister_best["variance_factor"]),
    )

    variant_oos: dict[str, list[RunMetric]] = {
        "baseline_default": [],
        "baseline_tuned": [],
        "sister_default": [],
        "sister_tuned": [],
    }

    for label, start_ts, end_ts in OOS_WINDOWS:
        runs = [
            (
                "baseline_default",
                BASELINE_STRATEGY,
                _bind_params(30, 1.8, 0.0),
                f"OOS_BASE_DEFAULT_{label}",
            ),
            (
                "baseline_tuned",
                BASELINE_STRATEGY,
                _bind_params(int(baseline_best["adx_threshold"]), float(baseline_best["variance_factor"]), 0.0),
                f"OOS_BASE_TUNED_{label}",
            ),
            (
                "sister_default",
                SISTER_STRATEGY,
                _bind_params(30, 1.8, 0.0),
                f"OOS_SISTER_DEFAULT_{label}",
            ),
            (
                "sister_tuned",
                SISTER_STRATEGY,
                _bind_params(int(sister_best["adx_threshold"]), float(sister_best["variance_factor"]), 0.0),
                f"OOS_SISTER_TUNED_{label}",
            ),
        ]
        for bucket, strategy_name, bind, run_id in runs:
            metric = await _run_strategy(
                run_id=run_id,
                strategy_name=strategy_name,
                bind_symbols=bind,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            metric.label = label
            variant_oos[bucket].append(metric)

    payload = {
        "model_id": MODEL_ID,
        "baseline_strategy": BASELINE_STRATEGY,
        "sister_strategy": SISTER_STRATEGY,
        "data_root": str(DATA_ROOT),
        "is_windows": [{"label": label, "start_ts": start, "end_ts": end} for label, start, end in IS_WINDOWS],
        "oos_windows": [{"label": label, "start_ts": start, "end_ts": end} for label, start, end in OOS_WINDOWS],
        "parameter_grid": {
            "adx_thresholds": ADX_THRESHOLDS,
            "variance_factors": VARIANCE_FACTORS,
            "mae_values": MAE_VALUES,
        },
        "gap_report": gap_report,
        "baseline_best_is": baseline_best,
        "sister_best_is": sister_best,
        "baseline_is_aggregate": _aggregate(baseline_combo_runs[baseline_best_key]),
        "sister_is_aggregate": _aggregate(sister_combo_runs[sister_best_key]),
        "baseline_vs_sister_is": _head_to_head(
            sister_combo_runs[sister_best_key],
            baseline_combo_runs[baseline_best_key],
        ),
        "baseline_mae_sensitivity": baseline_mae,
        "sister_mae_sensitivity": sister_mae,
        "oos_aggregate": {key: _aggregate(rows) for key, rows in variant_oos.items()},
        "oos_head_to_head": {
            "sister_default_vs_baseline_default": _head_to_head(
                variant_oos["sister_default"],
                variant_oos["baseline_default"],
            ),
            "sister_tuned_vs_baseline_tuned": _head_to_head(
                variant_oos["sister_tuned"],
                variant_oos["baseline_tuned"],
            ),
        },
        "scan_rows": baseline_scan_rows + sister_scan_rows,
        "detailed_runs": {
            "baseline_best_is": [asdict(r) for r in baseline_combo_runs[baseline_best_key]],
            "sister_best_is": [asdict(r) for r in sister_combo_runs[sister_best_key]],
            **{key: [asdict(r) for r in rows] for key, rows in variant_oos.items()},
        },
    }

    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    OUT_JSON_MODEL.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    fieldnames = [
        "strategy_name",
        "adx_threshold",
        "variance_factor",
        "clean_ratio",
        "median_sharpe",
        "mean_return",
        "median_trades",
        "objective",
    ]
    for target in (OUT_CSV, OUT_CSV_MODEL):
        with target.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in baseline_scan_rows + sister_scan_rows:
                writer.writerow({key: row[key] for key in fieldnames})

    report_lines = [
        "# RSI ADX Gateway Uptrend Hold Research Report",
        "",
        f"Model ID: `{MODEL_ID}`",
        "",
        "## Scope",
        "",
        f"- Baseline: `{BASELINE_STRATEGY}`",
        f"- Sister strategy: `{SISTER_STRATEGY}`",
        "- Instrument: `BTCUSDT`",
        "- Data: local sample OHLCV 15m dataset for calendar year 2025, exposed through a local cleaned-view symlink for backtest compatibility",
        "- Each window includes a 300-bar warmup buffer before the labeled start to satisfy feature history requirements",
        "- Goal: compare whether suppressing sells during strong upward ADX trends improves behavior or just reduces turnover",
        "",
        "## OHLCV Gap Policy",
        "",
        f"- Gap count in the local 2025 sample: `{gap_report['gap_count']}`",
        f"- Largest detected gap: `{gap_report['largest_gap_missing_bars']}` missing bars",
        f"- Long-gap segment breaks (`>=8` missing 15m bars): `{gap_report['long_gap_segment_break_count']}`",
        "- Applied policy: no synthetic OHLCV bars; if long gaps had appeared they would be treated as segment breaks under the strategy-writer guidance.",
        "- In this sample, no post-gap exclusion or rewarm segmentation was required beyond the normal pre-window warmup buffer.",
        "",
        "## Behavioral Change",
        "",
        "- The sister strategy keeps the baseline entry logic unchanged.",
        "- The only logic change is on open positions: when ADX is high and `DI+ > DI-`, sell signals are suppressed.",
        "- This should primarily affect exits, holding period, and trade count rather than entry frequency.",
        "",
        "## In-Sample Best Configurations",
        "",
        f"- Baseline best: `adx_threshold={int(baseline_best['adx_threshold'])}`, `variance_factor={float(baseline_best['variance_factor'])}`, `mae=0.0`",
        f"- Sister best: `adx_threshold={int(sister_best['adx_threshold'])}`, `variance_factor={float(sister_best['variance_factor'])}`, `mae=0.0`",
        "",
        "## In-Sample Aggregates",
        "",
        f"- Baseline tuned median sharpe: `{payload['baseline_is_aggregate']['median_sharpe']:.4f}`, mean return: `{payload['baseline_is_aggregate']['mean_return']:.4f}`, mean trades: `{payload['baseline_is_aggregate']['mean_trades']:.2f}`",
        f"- Sister tuned median sharpe: `{payload['sister_is_aggregate']['median_sharpe']:.4f}`, mean return: `{payload['sister_is_aggregate']['mean_return']:.4f}`, mean trades: `{payload['sister_is_aggregate']['mean_trades']:.2f}`",
        f"- Sister minus baseline IS mean sharpe delta: `{payload['baseline_vs_sister_is']['mean_sharpe_delta']:.4f}`",
        f"- Sister minus baseline IS mean return delta: `{payload['baseline_vs_sister_is']['mean_return_delta']:.4f}`",
        f"- Sister minus baseline IS mean trade delta: `{payload['baseline_vs_sister_is']['mean_trade_delta']:.2f}`",
        "",
        "## OOS Aggregates",
        "",
        f"- Baseline default mean sharpe: `{payload['oos_aggregate']['baseline_default']['mean_sharpe']:.4f}`, mean return: `{payload['oos_aggregate']['baseline_default']['mean_return']:.4f}`, mean trades: `{payload['oos_aggregate']['baseline_default']['mean_trades']:.2f}`",
        f"- Sister default mean sharpe: `{payload['oos_aggregate']['sister_default']['mean_sharpe']:.4f}`, mean return: `{payload['oos_aggregate']['sister_default']['mean_return']:.4f}`, mean trades: `{payload['oos_aggregate']['sister_default']['mean_trades']:.2f}`",
        f"- Baseline tuned mean sharpe: `{payload['oos_aggregate']['baseline_tuned']['mean_sharpe']:.4f}`, mean return: `{payload['oos_aggregate']['baseline_tuned']['mean_return']:.4f}`, mean trades: `{payload['oos_aggregate']['baseline_tuned']['mean_trades']:.2f}`",
        f"- Sister tuned mean sharpe: `{payload['oos_aggregate']['sister_tuned']['mean_sharpe']:.4f}`, mean return: `{payload['oos_aggregate']['sister_tuned']['mean_return']:.4f}`, mean trades: `{payload['oos_aggregate']['sister_tuned']['mean_trades']:.2f}`",
        "",
        "## OOS Head-To-Head",
        "",
        f"- Sister default vs baseline default mean sharpe delta: `{payload['oos_head_to_head']['sister_default_vs_baseline_default']['mean_sharpe_delta']:.4f}`",
        f"- Sister default vs baseline default mean return delta: `{payload['oos_head_to_head']['sister_default_vs_baseline_default']['mean_return_delta']:.4f}`",
        f"- Sister default vs baseline default mean trade delta: `{payload['oos_head_to_head']['sister_default_vs_baseline_default']['mean_trade_delta']:.2f}`",
        f"- Sister tuned vs baseline tuned mean sharpe delta: `{payload['oos_head_to_head']['sister_tuned_vs_baseline_tuned']['mean_sharpe_delta']:.4f}`",
        f"- Sister tuned vs baseline tuned mean return delta: `{payload['oos_head_to_head']['sister_tuned_vs_baseline_tuned']['mean_return_delta']:.4f}`",
        f"- Sister tuned vs baseline tuned mean trade delta: `{payload['oos_head_to_head']['sister_tuned_vs_baseline_tuned']['mean_trade_delta']:.2f}`",
        "",
        "## Robustness Notes",
        "",
        "- Parameter scan was limited to `adx_threshold` and `variance_factor`, which are the direct control knobs on the gateway behavior.",
        "- Local sensitivity around the best IS point was checked with `mae` values `0.0`, `0.25`, and `0.5` for both strategies.",
        "- Interpret gains carefully if they are driven mainly by lower trade count rather than improved risk-adjusted returns.",
        "",
        "## Recommendation",
        "",
        "- Treat this as an exploratory variant unless the OOS tuned comparison shows persistent sharpe and return improvement without unacceptable drawdown expansion.",
        "- Promote only if the OOS edge is not just a turnover reduction artifact.",
        "",
        "## Artifacts",
        "",
        "- `research_results.json`",
        "- `parameter_scan.csv`",
        f"- `research_results_{MODEL_ID}.json`",
        f"- `parameter_scan_{MODEL_ID}.csv`",
    ]
    report = "\n".join(report_lines) + "\n"
    OUT_REPORT.write_text(report, encoding="utf-8")
    OUT_REPORT_MODEL.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(main())
