from __future__ import annotations

"""
Generic template for a strategy-family research runner.

Copy this into:
- skills/Strategy_Writer/scripts/<strategy_slug>/run_research.py

Then point the docs wrapper at it:
- docs/strategies/<strategy_slug>/run_research.py

Expected outputs:
- docs/strategies/<strategy_slug>/research_results.json
- docs/strategies/<strategy_slug>/parameter_scan.csv
- docs/strategies/<strategy_slug>/<strategy_slug>_research_report.md
"""

import asyncio
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean, median

from apps.run_code.backtest_app import run_backtest_app
from quant_engine.utils.paths import data_root_from_file


REPO_ROOT = Path(__file__).resolve().parents[4]
DOC_ROOT = REPO_ROOT / "docs/strategies/<strategy_slug>"
DATA_ROOT = data_root_from_file("apps/run_backtest.py", levels_up=1)

PRIMARY_STRATEGY = "<NEW_STRATEGY_NAME>"
REFERENCE_STRATEGIES = ["<REFERENCE_STRATEGY_NAME>"]
BASE_BIND = {"A": "BTCUSDT", "window_RSI": "14", "window_ADX": "14", "window_RSI_rolling": "5"}

OUT_JSON = DOC_ROOT / "research_results.json"
OUT_CSV = DOC_ROOT / "parameter_scan.csv"
OUT_MD = DOC_ROOT / "<strategy_slug>_research_report.md"

IS_WINDOWS = []
OOS_WINDOWS = []


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


async def run_once(*, run_id: str, strategy_name: str, bind_symbols: dict[str, str], start_ts: int, end_ts: int) -> RunMetric:
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


def aggregate(rows: list[RunMetric]) -> dict[str, float]:
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
    }


async def main() -> None:
    payload = {
        "primary_strategy": PRIMARY_STRATEGY,
        "reference_strategies": REFERENCE_STRATEGIES,
        "base_bind": BASE_BIND,
        "is_windows": IS_WINDOWS,
        "oos_windows": OOS_WINDOWS,
        "notes": {
            "todo": "Fill in scan logic, head-to-head comparison, regime notes, and report generation.",
        },
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    with OUT_CSV.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow(["parameter_a", "parameter_b", "metric"])
    OUT_MD.write_text(
        "# Research Report\n\nReplace this template with actual scan, OOS, robustness, and recommendation sections.\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    asyncio.run(main())
