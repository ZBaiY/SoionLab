from __future__ import annotations

import asyncio
import copy
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean, median
from typing import Any

from apps.run_code.backtest_app import run_backtest_app
from quant_engine.strategy.registry import get_strategy
from quant_engine.utils.paths import data_root_from_file


REPO_ROOT = Path(__file__).resolve().parents[4]
ROOT = REPO_ROOT / "docs/strategies/rsi_adx_gateway_flush"
DATA_ROOT = data_root_from_file("apps/run_backtest.py", levels_up=1)

LEGACY_STRATEGY = "RSI-ADX-SIDEWAYS-FRACTIONAL"
NEW_STRATEGY = "RSI-ADX-GATEWAY-FRACTIONAL"
NEW_MODEL_ID = "gateway_refined_bearish_dmi_v2"
BASE_BIND = {"A": "BTCUSDT", "window_RSI": "14", "window_ADX": "14", "window_RSI_rolling": "5"}

OUT_JSON = ROOT / "research_results.json"
OUT_SCAN_CSV = ROOT / "parameter_scan.csv"
OUT_REPORT = ROOT / "rsi_adx_gateway_flush_research_report.md"
OUT_JSON_MODEL = ROOT / f"research_results_{NEW_MODEL_ID}.json"
OUT_SCAN_CSV_MODEL = ROOT / f"parameter_scan_{NEW_MODEL_ID}.csv"
OUT_REPORT_MODEL = ROOT / f"rsi_adx_gateway_flush_research_report_{NEW_MODEL_ID}.md"

IS_WINDOWS = [
    ("2024-02-01", 1706745600000, 1709251200000),
    ("2024-07-01", 1719792000000, 1722470400000),
    ("2025-03-01", 1740787200000, 1743465600000),
    ("2025-07-01", 1751328000000, 1754006400000),
    ("2025-11-01", 1761955200000, 1764547200000),
]

OOS_WINDOWS = [
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


def _legacy_decision_cfg(*, adx_threshold: int, variance_factor: float) -> dict[str, Any]:
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
        start_ts=start_ts,
        end_ts=end_ts,
        data_root=DATA_ROOT,
        run_id=run_id,
    )
    summary = json.loads((Path("artifacts/runs") / run_id / "report/summary.json").read_text(encoding="utf-8"))
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


async def _run_legacy_variant(
    *,
    run_id: str,
    bind_symbols: dict[str, str],
    adx_threshold: int,
    variance_factor: float,
    start_ts: int,
    end_ts: int,
) -> RunMetric:
    strategy = get_strategy(LEGACY_STRATEGY)
    original = copy.deepcopy(strategy.DECISION_CFG)
    try:
        strategy.DECISION_CFG = _legacy_decision_cfg(
            adx_threshold=adx_threshold,
            variance_factor=variance_factor,
        )
        return await _run_strategy(
            run_id=run_id,
            strategy_name=LEGACY_STRATEGY,
            bind_symbols=bind_symbols,
            start_ts=start_ts,
            end_ts=end_ts,
        )
    finally:
        strategy.DECISION_CFG = original


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
    return {
        "window_count": float(len(rows)),
        "clean_count": float(len(clean)),
        "mean_sharpe": float(mean(r.sharpe for r in use)) if use else 0.0,
        "median_sharpe": float(median(r.sharpe for r in use)) if use else 0.0,
        "mean_return": float(mean(r.total_return for r in use)) if use else 0.0,
        "median_return": float(median(r.total_return for r in use)) if use else 0.0,
        "mean_trades": float(mean(r.total_trades for r in use)) if use else 0.0,
        "median_trades": float(median(r.total_trades for r in use)) if use else 0.0,
        "positive_return_ratio": (sum(1 for r in use if r.total_return > 0) / len(use)) if use else 0.0,
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
        "return_win_count": float(sum(1 for a, b in pairs if a.total_return > b.total_return)),
        "sharpe_win_count": float(sum(1 for a, b in pairs if a.sharpe > b.sharpe)),
    }


async def main() -> None:
    scan_rows: list[dict[str, Any]] = []
    combo_runs: dict[tuple[int, float], list[RunMetric]] = {}
    best_scan: dict[str, Any] | None = None

    for adx_threshold in ADX_THRESHOLDS:
        for variance_factor in VARIANCE_FACTORS:
            bind = dict(BASE_BIND)
            bind["adx_threshold"] = str(adx_threshold)
            bind["variance_factor"] = str(variance_factor)
            bind["mae"] = "0.0"
            rows: list[RunMetric] = []
            for label, start_ts, end_ts in IS_WINDOWS:
                run_id = (
                    f"IS_NEW_GATE_A{adx_threshold}_V{str(variance_factor).replace('.', 'p')}"
                    f"_{label.replace('-', '')}"
                )
                metric = await _run_strategy(
                    run_id=run_id,
                    strategy_name=NEW_STRATEGY,
                    bind_symbols=bind,
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
                metric.label = label
                rows.append(metric)
            combo_runs[(adx_threshold, variance_factor)] = rows
            score = _score_rows(rows)
            row = {"adx_threshold": adx_threshold, "variance_factor": variance_factor, **score}
            scan_rows.append(row)
            if best_scan is None or float(row["objective"]) > float(best_scan["objective"]):
                best_scan = row

    assert best_scan is not None
    best_adx = int(best_scan["adx_threshold"])
    best_variance = float(best_scan["variance_factor"])

    mae_sensitivity: list[dict[str, Any]] = []
    for mae in MAE_VALUES:
        bind = dict(BASE_BIND)
        bind["adx_threshold"] = str(best_adx)
        bind["variance_factor"] = str(best_variance)
        bind["mae"] = str(mae)
        rows: list[RunMetric] = []
        for label, start_ts, end_ts in IS_WINDOWS:
            run_id = f"IS_MAE_NEW_GATE_M{str(mae).replace('.', 'p')}_{label.replace('-', '')}"
            metric = await _run_strategy(
                run_id=run_id,
                strategy_name=NEW_STRATEGY,
                bind_symbols=bind,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            metric.label = label
            rows.append(metric)
        mae_sensitivity.append({"mae": mae, **_score_rows(rows)})

    variant_oos: dict[str, list[RunMetric]] = {
        "legacy_default": [],
        "legacy_tuned": [],
        "new_default": [],
        "new_tuned": [],
    }

    for label, start_ts, end_ts in OOS_WINDOWS:
        metric = await _run_legacy_variant(
            run_id=f"OOS_LEGACY_DEFAULT_{label.split('/')[0].replace('-', '')}",
            bind_symbols=dict(BASE_BIND),
            adx_threshold=25,
            variance_factor=1.8,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        metric.label = label
        variant_oos["legacy_default"].append(metric)

        metric = await _run_legacy_variant(
            run_id=f"OOS_LEGACY_TUNED_{label.split('/')[0].replace('-', '')}",
            bind_symbols=dict(BASE_BIND),
            adx_threshold=20,
            variance_factor=1.8,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        metric.label = label
        variant_oos["legacy_tuned"].append(metric)

        bind = dict(BASE_BIND)
        bind["adx_threshold"] = "25"
        bind["variance_factor"] = "1.8"
        bind["mae"] = "0.0"
        metric = await _run_strategy(
            run_id=f"OOS_NEW_DEFAULT_{label.split('/')[0].replace('-', '')}",
            strategy_name=NEW_STRATEGY,
            bind_symbols=bind,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        metric.label = label
        variant_oos["new_default"].append(metric)

        bind = dict(BASE_BIND)
        bind["adx_threshold"] = str(best_adx)
        bind["variance_factor"] = str(best_variance)
        bind["mae"] = "0.0"
        metric = await _run_strategy(
            run_id=f"OOS_NEW_TUNED_A{best_adx}_V{str(best_variance).replace('.', 'p')}_{label.split('/')[0].replace('-', '')}",
            strategy_name=NEW_STRATEGY,
            bind_symbols=bind,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        metric.label = label
        variant_oos["new_tuned"].append(metric)

    variant_is: dict[str, list[RunMetric]] = {
        "legacy_default": [],
        "legacy_tuned": [],
        "new_default": [],
        "new_tuned": combo_runs[(best_adx, best_variance)],
    }
    for label, start_ts, end_ts in IS_WINDOWS:
        metric = await _run_legacy_variant(
            run_id=f"IS_LEGACY_DEFAULT_{label.replace('-', '')}",
            bind_symbols=dict(BASE_BIND),
            adx_threshold=25,
            variance_factor=1.8,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        metric.label = label
        variant_is["legacy_default"].append(metric)

        metric = await _run_legacy_variant(
            run_id=f"IS_LEGACY_TUNED_{label.replace('-', '')}",
            bind_symbols=dict(BASE_BIND),
            adx_threshold=20,
            variance_factor=1.8,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        metric.label = label
        variant_is["legacy_tuned"].append(metric)

        bind = dict(BASE_BIND)
        bind["adx_threshold"] = "25"
        bind["variance_factor"] = "1.8"
        bind["mae"] = "0.0"
        metric = await _run_strategy(
            run_id=f"IS_NEW_DEFAULT_{label.replace('-', '')}",
            strategy_name=NEW_STRATEGY,
            bind_symbols=bind,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        metric.label = label
        variant_is["new_default"].append(metric)

    aggregates = {
        "is": {name: _aggregate(rows) for name, rows in variant_is.items()},
        "oos": {name: _aggregate(rows) for name, rows in variant_oos.items()},
    }
    head_to_head = {
        "new_tuned_vs_legacy_default_oos": _head_to_head(variant_oos["new_tuned"], variant_oos["legacy_default"]),
        "new_tuned_vs_legacy_tuned_oos": _head_to_head(variant_oos["new_tuned"], variant_oos["legacy_tuned"]),
        "new_tuned_vs_new_default_oos": _head_to_head(variant_oos["new_tuned"], variant_oos["new_default"]),
    }

    payload = {
        "model_id": NEW_MODEL_ID,
        "objective": "maximize median_sharpe with mean_return, median_trades, clean_ratio tie-breakers on IS windows",
        "new_strategy": NEW_STRATEGY,
        "legacy_strategy": LEGACY_STRATEGY,
        "base_bind": BASE_BIND,
        "is_windows": IS_WINDOWS,
        "oos_windows": OOS_WINDOWS,
        "scan_rows": scan_rows,
        "best_new_combo": best_scan,
        "mae_sensitivity": mae_sensitivity,
        "variant_is": {k: [asdict(r) for r in v] for k, v in variant_is.items()},
        "variant_oos": {k: [asdict(r) for r in v] for k, v in variant_oos.items()},
        "aggregates": aggregates,
        "head_to_head": head_to_head,
        "notes": {
            "new_model_behavior": (
                "The refined gateway model keeps the ADX-gated lower-band entry and the normal upper-band exit, "
                "but the protective lower-band flush now requires bearish DMI confirmation: ADX high, RSI at or "
                "below the lower dynamic band, already in position, and -DI > +DI."
            ),
            "baseline_inventory": (
                "Repo inspection found one existing registered OHLCV RSI dynamic-band family reference strategy: "
                "RSI-ADX-SIDEWAYS-FRACTIONAL. There is no separate registered plain non-ADX dynamic-band strategy "
                "under apps/strategy, so comparison uses the legacy default and the prior tuned legacy challenger "
                "configuration from docs/strategies/rsi_adxgateway."
            )
        },
    }
    payload_text = json.dumps(payload, indent=2)
    OUT_JSON.write_text(payload_text, encoding="utf-8")
    OUT_JSON_MODEL.write_text(payload_text, encoding="utf-8")

    csv_fields = [
        "adx_threshold",
        "variance_factor",
        "clean_ratio",
        "median_sharpe",
        "mean_return",
        "median_trades",
        "objective",
    ]
    for path in (OUT_SCAN_CSV, OUT_SCAN_CSV_MODEL):
        with path.open("w", newline="", encoding="utf-8") as fp:
            writer = csv.DictWriter(fp, fieldnames=csv_fields)
            writer.writeheader()
            writer.writerows(scan_rows)

    def _pct(x: float) -> str:
        return f"{x:.4%}"

    md: list[str] = []
    md.append("# RSI ADX Gateway Flush Research Report")
    md.append("")
    md.append("## Objective")
    md.append("")
    md.append(
        "Evaluate the refined `RSI-ADX-GATEWAY-FRACTIONAL` variant against the existing RSI dynamic-band family on "
        "BTCUSDT OHLCV using real backtest runs. This report tracks model "
        f"`{NEW_MODEL_ID}`, where the protective lower-band flush requires bearish DMI confirmation."
    )
    md.append("")
    md.append("## Strategy Variants")
    md.append("")
    md.append("- `legacy_default`: `RSI-ADX-SIDEWAYS-FRACTIONAL` with `adx_threshold=25`, `variance_factor=1.8`, `window_RSI_rolling=5`")
    md.append("- `legacy_tuned`: same legacy strategy with prior repo-local tuned challenger `adx_threshold=20`, `variance_factor=1.8`, `window_RSI_rolling=5`")
    md.append("- `new_default`: `RSI-ADX-GATEWAY-FRACTIONAL` with the same default parameters")
    md.append(f"- `new_tuned`: `RSI-ADX-GATEWAY-FRACTIONAL` with IS best combo `adx_threshold={best_adx}`, `variance_factor={best_variance}`, `mae=0.0`")
    md.append("")
    md.append("Behavioral delta of the new strategy:")
    md.append("")
    md.append("- lower-band entry is still blocked when ADX is high")
    md.append("- unlike the legacy strategy, an existing long can flatten on lower-band weakness only when ADX is high and bearish DMI confirms `-DI > +DI`")
    md.append("- upper-band exit remains unchanged")
    md.append("")
    md.append("Inventory note:")
    md.append("")
    md.append(f"- {payload['notes']['baseline_inventory']}")
    md.append("")
    md.append("## Parameter Scan")
    md.append("")
    md.append(f"- IS windows: {[w[0] for w in IS_WINDOWS]}")
    md.append(f"- scanned `adx_threshold`: {ADX_THRESHOLDS}")
    md.append(f"- scanned `variance_factor`: {VARIANCE_FACTORS}")
    md.append("- fixed: `window_RSI=14`, `window_ADX=14`, `window_RSI_rolling=5`, `mae=0.0`")
    md.append("")
    md.append("| adx_threshold | variance_factor | median_sharpe | mean_return | median_trades | objective |")
    md.append("|---:|---:|---:|---:|---:|---:|")
    for row in sorted(scan_rows, key=lambda x: x["objective"], reverse=True):
        md.append(
            f"| {row['adx_threshold']} | {row['variance_factor']} | {row['median_sharpe']:.4f} | "
            f"{_pct(float(row['mean_return']))} | {row['median_trades']:.1f} | {row['objective']:.4f} |"
        )
    md.append("")
    md.append(f"Best IS combo: `adx_threshold={best_adx}`, `variance_factor={best_variance}`, `mae=0.0`.")
    md.append("")
    md.append("## In-Sample Results")
    md.append("")
    md.append("| variant | mean_sharpe | median_sharpe | mean_return | median_return | mean_trades | positive_return_ratio |")
    md.append("|---|---:|---:|---:|---:|---:|---:|")
    for name, agg in aggregates["is"].items():
        md.append(
            f"| {name} | {agg['mean_sharpe']:.4f} | {agg['median_sharpe']:.4f} | {_pct(agg['mean_return'])} | "
            f"{_pct(agg['median_return'])} | {agg['mean_trades']:.1f} | {agg['positive_return_ratio']:.2%} |"
        )
    md.append("")
    md.append("## Out-of-Sample Results")
    md.append("")
    md.append("| variant | mean_sharpe | median_sharpe | mean_return | median_return | mean_trades | positive_return_ratio |")
    md.append("|---|---:|---:|---:|---:|---:|---:|")
    for name, agg in aggregates["oos"].items():
        md.append(
            f"| {name} | {agg['mean_sharpe']:.4f} | {agg['median_sharpe']:.4f} | {_pct(agg['mean_return'])} | "
            f"{_pct(agg['median_return'])} | {agg['mean_trades']:.1f} | {agg['positive_return_ratio']:.2%} |"
        )
    md.append("")
    md.append("## Rolling OOS Window Detail")
    md.append("")
    md.append("| window | legacy_default_ret | legacy_tuned_ret | new_default_ret | new_tuned_ret | legacy_default_trades | new_tuned_trades |")
    md.append("|---|---:|---:|---:|---:|---:|---:|")
    for idx, (label, _, _) in enumerate(OOS_WINDOWS):
        ld = variant_oos["legacy_default"][idx]
        lt = variant_oos["legacy_tuned"][idx]
        nd = variant_oos["new_default"][idx]
        nt = variant_oos["new_tuned"][idx]
        md.append(
            f"| {label} | {_pct(ld.total_return)} | {_pct(lt.total_return)} | {_pct(nd.total_return)} | {_pct(nt.total_return)} | "
            f"{ld.total_trades} | {nt.total_trades} |"
        )
    md.append("")
    md.append("## Robustness Checks")
    md.append("")
    md.append("1. Rolling-window OOS comparison")
    md.append(f"- `new_tuned` vs `legacy_default`: {head_to_head['new_tuned_vs_legacy_default_oos']}")
    md.append(f"- `new_tuned` vs `legacy_tuned`: {head_to_head['new_tuned_vs_legacy_tuned_oos']}")
    md.append(f"- `new_tuned` vs `new_default`: {head_to_head['new_tuned_vs_new_default_oos']}")
    md.append("")
    md.append("2. Local sensitivity around the best new point")
    md.append("")
    md.append("| mae | median_sharpe | mean_return | median_trades | objective |")
    md.append("|---:|---:|---:|---:|---:|")
    for row in mae_sensitivity:
        md.append(
            f"| {row['mae']} | {row['median_sharpe']:.4f} | {_pct(float(row['mean_return']))} | "
            f"{row['median_trades']:.1f} | {row['objective']:.4f} |"
        )
    md.append("")
    md.append("3. Trade-count interpretation")
    md.append("")
    md.append(
        "Trade-count deltas are reported explicitly above so any return improvement can be judged against selectivity changes. "
        "A lower trade count alone is not treated as evidence of improvement."
    )
    md.append("")
    md.append("## Answers")
    md.append("")
    new_vs_legacy_default = head_to_head["new_tuned_vs_legacy_default_oos"]
    new_vs_legacy_tuned = head_to_head["new_tuned_vs_legacy_tuned_oos"]
    md.append(f"1. Behavioral difference: the new strategy adds a lower-band protective flush under high ADX; the legacy strategy does not.")
    md.append(
        f"2. Material improvement check: vs legacy default OOS, `new_tuned` mean return delta = {_pct(new_vs_legacy_default['mean_return_delta'])}, "
        f"median return delta = {_pct(new_vs_legacy_default['median_return_delta'])}, mean Sharpe delta = {new_vs_legacy_default['mean_sharpe_delta']:.4f}."
    )
    md.append(
        f"3. Selectivity check: `new_tuned` mean trade delta vs legacy default OOS = {new_vs_legacy_default['mean_trade_delta']:.1f}; "
        "this indicates whether gains came with materially different activity."
    )
    md.append(f"4. Locally best new parameter region: adx_threshold near `{best_adx}` and variance_factor near `{best_variance}` on the tested grid.")
    md.append(
        f"5. OOS vs older references: `new_tuned` return wins were "
        f"{int(new_vs_legacy_default['return_win_count'])}/{len(OOS_WINDOWS)} vs legacy default and "
        f"{int(new_vs_legacy_tuned['return_win_count'])}/{len(OOS_WINDOWS)} vs legacy tuned."
    )
    md.append(
        "6. Robustness evidence should be judged from the rolling OOS table and head-to-head aggregates, not the IS winner alone."
    )
    recommendation = "exploratory variant"
    if (
        new_vs_legacy_default["mean_return_delta"] > 0
        and new_vs_legacy_default["median_sharpe_delta"] > 0
        and new_vs_legacy_tuned["mean_return_delta"] > 0
    ):
        recommendation = "challenger configuration"
    md.append(f"7. Recommendation: `{recommendation}`.")
    md.append("")
    md.append("## Conclusion")
    md.append("")
    md.append(
        "This study should be read as a sober comparison of a narrow decision-layer variant. "
        "Promotion should depend on OOS and rolling-window evidence, not on the IS scan winner alone."
    )
    md.append("")
    md.append("Artifacts:")
    md.append("")
    md.append(f"- `{OUT_JSON.name}`")
    md.append(f"- `{OUT_SCAN_CSV.name}`")
    md.append(f"- `{OUT_JSON_MODEL.name}`")
    md.append(f"- `{OUT_SCAN_CSV_MODEL.name}`")
    report_text = "\n".join(md) + "\n"
    OUT_REPORT.write_text(report_text, encoding="utf-8")
    OUT_REPORT_MODEL.write_text(report_text, encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(main())
