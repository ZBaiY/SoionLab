from __future__ import annotations

import asyncio
import json
from pathlib import Path

from apps.run_code.backtest_app import run_backtest_app
from quant_engine.utils.paths import data_root_from_file


DATA_ROOT = data_root_from_file("apps/run_backtest.py", levels_up=1)
REPO_ROOT = Path(__file__).resolve().parents[4]
DOC_DIR = REPO_ROOT / "docs/strategies/rsi_iv"
RESULTS_JSON = DOC_DIR / "relative_state_experiment_results.json"
REPORT_MD = DOC_DIR / "relative_state_experiment_report.md"

# Current local option-chain slice used elsewhere in this repo work.
START_TS = 1769510562229
END_TS = 1769979299115

VARIANTS = [
    {
        "label": "Baseline",
        "strategy_name": "RSI-ADX-SIDEWAYS-FRACTIONAL",
        "run_id": "EXP_RSI_ADX_BASELINE_20260309",
        "bind_symbols": {
            "A": "BTCUSDT",
            "window_RSI": "14",
            "window_ADX": "14",
            "window_RSI_rolling": "5",
        },
        "notes": "Existing RSI dynamic-band + ADX gate baseline.",
    },
    {
        "label": "Challenger 7d",
        "strategy_name": "RSI-IV-RELSTATE-ADX-SIDEWAY-FRACTIONAL",
        "run_id": "EXP_RSI_IV_RELSTATE_7D_20260309",
        "bind_symbols": {
            "A": "BTCUSDT",
            "window_RSI": "14",
            "window_ADX": "14",
            "window_RSI_rolling": "5",
            "iv_x_target": "0.0",
            "iv_tau_days": "7",
        },
        "notes": "ATM 7d mark_iv relative state, bounded width multiplier.",
    },
    {
        "label": "Challenger 3d",
        "strategy_name": "RSI-IV-RELSTATE-ADX-SIDEWAY-FRACTIONAL",
        "run_id": "EXP_RSI_IV_RELSTATE_3D_20260309",
        "bind_symbols": {
            "A": "BTCUSDT",
            "window_RSI": "14",
            "window_ADX": "14",
            "window_RSI_rolling": "5",
            "iv_x_target": "0.0",
            "iv_tau_days": "3",
        },
        "notes": "ATM 3d mark_iv relative state, bounded width multiplier.",
    },
    {
        "label": "Challenger 14d",
        "strategy_name": "RSI-IV-RELSTATE-ADX-SIDEWAY-FRACTIONAL",
        "run_id": "EXP_RSI_IV_RELSTATE_14D_20260309",
        "bind_symbols": {
            "A": "BTCUSDT",
            "window_RSI": "14",
            "window_ADX": "14",
            "window_RSI_rolling": "5",
            "iv_x_target": "0.0",
            "iv_tau_days": "14",
        },
        "notes": "Optional slower ATM comparison.",
    },
]


async def _run_variant(spec: dict[str, object]) -> dict[str, object]:
    await run_backtest_app(
        strategy_name=str(spec["strategy_name"]),
        bind_symbols=dict(spec["bind_symbols"]), # type: ignore
        start_ts=int(START_TS),
        end_ts=int(END_TS),
        data_root=DATA_ROOT,
        run_id=str(spec["run_id"]),
    )
    summary_path = Path("artifacts/runs") / str(spec["run_id"]) / "report" / "summary.json"
    summary = json.loads(summary_path.read_text())
    perf = dict(summary.get("performance_summary") or {})
    return {
        "label": spec["label"],
        "strategy_name": spec["strategy_name"],
        "run_id": spec["run_id"],
        "notes": spec["notes"],
        "bind_symbols": dict(spec["bind_symbols"]), # type: ignore
        "sharpe": float(perf.get("sharpe") or 0.0),
        "total_return": float(perf.get("total_return") or 0.0),
        "total_trades": int(perf.get("total_trades") or 0),
        "max_drawdown": float(perf.get("max_drawdown") or 0.0),
        "quality_verdict": str(summary.get("quality_verdict") or "UNKNOWN"),
    }


def _fmt_pct(x: float) -> str:
    return f"{x * 100.0:.2f}%"


async def main() -> None:
    results = []
    for spec in VARIANTS:
        results.append(await _run_variant(spec))

    RESULTS_JSON.write_text(json.dumps(results, indent=2), encoding="utf-8")

    baseline = next(r for r in results if r["label"] == "Baseline")
    lines: list[str] = []
    lines.append("# RSI + IV Relative-State Dynamic Band Experiment")
    lines.append("")
    lines.append("## Hypothesis")
    lines.append("")
    lines.append(
        "Near-term ATM option IV may help adapt the width of the existing RSI dynamic band better than the fixed-width baseline. "
        "This experiment tests width adaptation only. It does not use IV as a directional predictor."
    )
    lines.append("")
    lines.append("## Experiment Variants")
    lines.append("")
    for row in results:
        lines.append(f"- `{row['label']}`: `{row['strategy_name']}`")
        lines.append(f"  {row['notes']}")
    lines.append("")
    lines.append("## IV Input Definition")
    lines.append("")
    lines.append("- Selection: ATM-like (`x = 0.0`) using the existing option-chain handler selection path")
    lines.append("- Maturity challengers: `3d`, `7d`, optional `14d`")
    lines.append("- Field: `mark_iv`")
    lines.append("- Relative state: `current_iv / median(recent_visible_selected_iv_history)`")
    lines.append("- Background window: last `24` visible selected snapshots, with the current value compared against the prior history")
    lines.append("")
    lines.append("## Dynamic-Band Formula")
    lines.append("")
    lines.append("- Baseline width: `variance_factor * rsi_std`")
    lines.append("- Challenger width: `variance_factor * rsi_std * clip(relative_iv, 0.75, 1.25)`")
    lines.append("- ADX gate and entry/exit semantics remain the same as the baseline")
    lines.append("")
    lines.append("## Results")
    lines.append("")
    lines.append("| Variant | Sharpe | Return | Trades | Max Drawdown | Verdict |")
    lines.append("|---|---:|---:|---:|---:|---|")
    for row in results:
        lines.append(
            f"| {row['label']} | {row['sharpe']:.4f} | {_fmt_pct(float(row['total_return']))} | "
            f"{row['total_trades']} | {_fmt_pct(float(row['max_drawdown']))} | {row['quality_verdict']} |"
        )
    lines.append("")
    lines.append("## Trade-Count Effects")
    lines.append("")
    for row in results:
        if row["label"] == "Baseline":
            continue
        trade_delta = int(row["total_trades"]) - int(baseline["total_trades"])
        ret_delta = float(row["total_return"]) - float(baseline["total_return"])
        sharpe_delta = float(row["sharpe"]) - float(baseline["sharpe"])
        lines.append(
            f"- `{row['label']}` vs baseline: trades `{trade_delta:+d}`, return `{_fmt_pct(ret_delta)}`, Sharpe `{sharpe_delta:+.4f}`"
        )
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    best = max(results[1:], key=lambda x: float(x["sharpe"])) if len(results) > 1 else baseline
    lines.append(
        "This is a narrow single-window experiment on the current local option-chain slice, so the result should be read as a feasibility check, not a promotion decision."
    )
    lines.append("")
    lines.append(
        f"Within this run set, the best challenger by Sharpe was `{best['label']}`."
    )
    lines.append("")
    if float(best["sharpe"]) > float(baseline["sharpe"]) and int(best["total_trades"]) > 0:
        lines.append(
            "The only reason to treat the challenger as interesting is if it improves risk-adjusted behavior without merely collapsing trade count."
        )
    else:
        lines.append(
            "If the challengers do not improve Sharpe/return beyond the baseline, or only do so by becoming nearly inactive, the IV-width idea is weak in this first form."
        )
    lines.append("")
    lines.append("## Recommendation")
    lines.append("")
    lines.append(
        "Use this result only to decide whether the idea deserves one more narrow iteration. Do not treat it as evidence for a broad IV-adaptive framework."
    )
    lines.append("")
    lines.append("Source summaries:")
    lines.append("")
    for row in results:
        lines.append(
            f"- [summary.json](/Users/zhaoyub/Documents/Tradings/SoionLab/artifacts/runs/{row['run_id']}/report/summary.json)"
        )
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(main())
