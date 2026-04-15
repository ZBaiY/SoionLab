from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean, median
from typing import Any

from apps.run_code.backtest_app import run_backtest_app
from quant_engine.utils.paths import data_root_from_file


DATA_ROOT = data_root_from_file("apps/run_backtest.py", levels_up=1)


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


async def run_strategy_once(
    *,
    run_id: str,
    strategy_name: str,
    bind_symbols: dict[str, str],
    start_ts: int,
    end_ts: int,
    data_root: str | Path | None = None,
) -> RunMetric:
    await run_backtest_app(
        strategy_name=strategy_name,
        bind_symbols=bind_symbols,
        start_ts=start_ts,
        end_ts=end_ts,
        data_root=str(data_root or DATA_ROOT),
        run_id=run_id,
    )
    summary_path = Path("artifacts/runs") / run_id / "report" / "summary.json"
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


def score_rows(rows: list[RunMetric]) -> dict[str, float]:
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


def aggregate_rows(rows: list[RunMetric]) -> dict[str, float]:
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
        "positive_return_ratio": float(sum(1 for r in use if r.total_return > 0) / len(use)),
    }


def head_to_head(lhs: list[RunMetric], rhs: list[RunMetric]) -> dict[str, float]:
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


def write_scan_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def serialize_metrics(rows: list[RunMetric]) -> list[dict[str, Any]]:
    return [asdict(row) for row in rows]

