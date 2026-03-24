from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median
from typing import Any

import pandas as pd

from apps.run_code.backtest_app import run_backtest_app


REPO_ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = REPO_ROOT / "data"
OUT_DIR = REPO_ROOT / "docs" / "strategies" / "rsi_adx_gateway_uptrend_hold"
OUT_JSON = OUT_DIR / "macd_hist_tuning_results.json"
OUT_MD = OUT_DIR / "macd_hist_tuning_report.md"

START_TS = 1679667299999 + 300 * 15 * 60 * 1000
END_TS = 1769904000000

BASE_STRATEGY = "RSI-ADX-GATEWAY-UPTREND-HOLD-FRACTIONAL"
BASE_BIND = {
    "A": "BTCUSDT",
    "window_RSI": "14",
    "window_ADX": "14",
    "window_RSI_rolling": "5",
    "adx_threshold": "30",
    "variance_factor": "1.8",
    "mae": "0.0",
}

WINDOWS = [
    ("2023-FULL-AVAILABLE", 1679667299999 + 300 * 15 * 60 * 1000, 1704067200000),
    ("2024-H1", 1704067200000, 1719792000000),
    ("2024-H2", 1719792000000, 1735689600000),
    ("2024-FULL", 1704067200000, 1735689600000),
    ("2025-H1", 1735689600000, 1751328000000),
    ("2025-H2", 1751328000000, 1767225600000),
    ("2025-FULL", 1735689600000, 1767225600000),
]

MACD_PARAM_GRID = [
    {"fast": 4, "slow": 9, "signal": 3},
    {"fast": 5, "slow": 15, "signal": 5},
    {"fast": 6, "slow": 13, "signal": 4},
    {"fast": 8, "slow": 17, "signal": 5},
]
RELEASE_BARS = [2, 3, 4, 5]

HARDCODED_BASELINES = {
    "hard_hold": {
        "2023-FULL-AVAILABLE": {"return": 0.2182, "sharpe": 1.28, "max_drawdown": -0.1309},
        "2024-H1": {"return": 0.4192278750, "sharpe": 2.3860890427, "max_drawdown": -0.1175998899},
        "2024-H2": {"return": 0.1440442689, "sharpe": 1.0142399806, "max_drawdown": -0.1664272059},
        "2024-FULL": {"return": 0.6212761155, "sharpe": 1.6963025991, "max_drawdown": -0.1664272059},
        "2025-H1": {"return": 0.1572526645, "sharpe": 1.0771258181, "max_drawdown": -0.1357385013},
        "2025-H2": {"return": 0.2546969988, "sharpe": 2.0148064183, "max_drawdown": -0.1377410337},
        "2025-FULL": {"return": 0.4520893432, "sharpe": 1.4629879408, "max_drawdown": -0.1571844815},
    },
    "hist3_default": {
        "2024-H1": {"return": 0.4314133255, "sharpe": 2.4479244858, "max_drawdown": -0.1175990970},
        "2024-H2": {"return": 0.1503982497, "sharpe": 1.0507285667, "max_drawdown": -0.1624016140},
        "2024-FULL": {"return": 0.6442779958, "sharpe": 1.7448002652, "max_drawdown": -0.1624016140},
        "2025-H1": {"return": 0.1623425247, "sharpe": 1.1089776153, "max_drawdown": -0.1357635032},
        "2025-H2": {"return": 0.2378895585, "sharpe": 1.9175408420, "max_drawdown": -0.1363056002},
        "2025-FULL": {"return": 0.4389387913, "sharpe": 1.4386082039, "max_drawdown": -0.1557855677},
    },
}


@dataclass
class WindowMetric:
    label: str
    total_return: float
    sharpe: float
    max_drawdown: float


def _annualized_sharpe(equity_curve: list[list[float]]) -> float:
    if len(equity_curve) < 2:
        return 0.0
    returns: list[float] = []
    for i in range(1, len(equity_curve)):
        prev = float(equity_curve[i - 1][1])
        cur = float(equity_curve[i][1])
        if prev > 0:
            returns.append(cur / prev - 1.0)
    if len(returns) < 2:
        return 0.0
    series = pd.Series(returns, dtype="float64")
    std = float(series.std(ddof=1))
    if std <= 0.0:
        return 0.0
    bars_per_year = 365.0 * 24.0 * 4.0
    return float(series.mean() / std * (bars_per_year ** 0.5))


def _max_drawdown(equity_curve: list[list[float]]) -> float:
    peak = None
    worst = 0.0
    for _, equity in equity_curve:
        eq = float(equity)
        peak = eq if peak is None else max(peak, eq)
        if peak and peak > 0.0:
            dd = eq / peak - 1.0
            worst = min(worst, dd)
    return float(worst)


def _slice_report(report_path: Path) -> dict[str, WindowMetric]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    curve = list(report.get("equity_curve") or [])
    if not curve:
        return {}
    result: dict[str, WindowMetric] = {}
    for label, start_ts, end_ts in WINDOWS:
        window_curve = [row for row in curve if int(row[0]) >= start_ts and int(row[0]) <= end_ts]
        if len(window_curve) < 2:
            continue
        start_eq = float(window_curve[0][1])
        end_eq = float(window_curve[-1][1])
        total_return = (end_eq / start_eq - 1.0) if start_eq > 0 else 0.0
        result[label] = WindowMetric(
            label=label,
            total_return=float(total_return),
            sharpe=_annualized_sharpe(window_curve),
            max_drawdown=_max_drawdown(window_curve),
        )
    return result


def _score(metrics: dict[str, WindowMetric]) -> dict[str, float]:
    use = [metrics[label] for label in ("2024-FULL", "2025-FULL", "2024-H1", "2024-H2", "2025-H1", "2025-H2") if label in metrics]
    if not use:
        return {"objective": -999.0}
    mean_return = float(mean(m.total_return for m in use))
    mean_sharpe = float(mean(m.sharpe for m in use))
    mean_drawdown = float(mean(m.max_drawdown for m in use))
    return {
        "mean_return": mean_return,
        "mean_sharpe": mean_sharpe,
        "mean_drawdown": mean_drawdown,
        "objective": mean_sharpe + 0.35 * mean_return + 0.10 * mean_drawdown,
    }


async def _run_one(*, fast: int, slow: int, signal: int, release_bars: int) -> dict[str, Any]:
    bind = dict(BASE_BIND)
    bind.update(
        {
            "window_MACD_fast": str(fast),
            "window_MACD_slow": str(slow),
            "window_MACD_signal": str(signal),
            "macd_hist_release_bars": str(release_bars),
        }
    )
    run_id = f"TUNE_UPHOLD_MACD_HIST_F{fast}_S{slow}_G{signal}_R{release_bars}"
    report_path = REPO_ROOT / "artifacts" / "runs" / run_id / "report" / "report.json"
    if not report_path.exists():
        await run_backtest_app(
            strategy_name=BASE_STRATEGY,
            bind_symbols=bind,
            start_ts=START_TS,
            end_ts=END_TS,
            data_root=DATA_ROOT,
            run_id=run_id,
        )
    metrics = _slice_report(report_path)
    score = _score(metrics)
    return {
        "run_id": run_id,
        "fast": fast,
        "slow": slow,
        "signal": signal,
        "release_bars": release_bars,
        "windows": {label: vars(metric) for label, metric in metrics.items()},
        "score": score,
    }


def _format_md(results: list[dict[str, Any]]) -> str:
    top = results[:10]
    lines = [
        "# MACD Histogram Tuning",
        "",
        "Fixed strategy params: `adx_threshold=30`, `variance_factor=1.8`, `mae=0.0`.",
        "",
        "## Top Candidates",
        "",
        "| rank | fast | slow | signal | release_bars | objective | 2024 full return | 2025 full return | 2025 full sharpe |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for idx, row in enumerate(top, start=1):
        windows = row["windows"]
        y24 = windows.get("2024-FULL", {})
        y25 = windows.get("2025-FULL", {})
        lines.append(
            f"| {idx} | {row['fast']} | {row['slow']} | {row['signal']} | {row['release_bars']} | "
            f"{row['score']['objective']:.4f} | {100*y24.get('total_return', 0.0):.2f}% | "
            f"{100*y25.get('total_return', 0.0):.2f}% | {y25.get('sharpe', 0.0):.3f} |"
        )
    return "\n".join(lines) + "\n"


async def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for params in MACD_PARAM_GRID:
        for release_bars in RELEASE_BARS:
            results.append(await _run_one(release_bars=release_bars, **params))
    results.sort(key=lambda row: row["score"]["objective"], reverse=True)
    payload = {
        "baseline_reference": HARDCODED_BASELINES,
        "results": results,
        "top_by_median": {
            "objective_median_top3": median(row["score"]["objective"] for row in results[:3]) if results else None,
        },
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    OUT_MD.write_text(_format_md(results), encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(main())
