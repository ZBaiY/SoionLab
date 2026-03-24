from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any


def run_dir(repo_root: Path, run_id: str) -> Path:
    return repo_root / "artifacts" / "runs" / run_id


def load_report(repo_root: Path, run_id: str) -> dict[str, Any]:
    path = run_dir(repo_root, run_id) / "report" / "report.json"
    return json.loads(path.read_text(encoding="utf-8"))


def load_summary(repo_root: Path, run_id: str) -> dict[str, Any]:
    path = run_dir(repo_root, run_id) / "report" / "summary.json"
    return json.loads(path.read_text(encoding="utf-8"))


def iter_trace_steps(repo_root: Path, run_id: str):
    path = run_dir(repo_root, run_id) / "logs" / "trace.jsonl"
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("event") == "engine.step.trace":
                yield obj


def parse_feature_key_map(raw: str | None) -> dict[str, str]:
    out: dict[str, str] = {}
    if not raw:
        return out
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        name, key = part.split("=", 1)
        out[name.strip()] = key.strip()
    return out


def first_present(features: dict[str, Any], keys: list[str]) -> float:
    for key in keys:
        if key in features and features.get(key) is not None:
            return safe_float(features.get(key))
    return 0.0


def extract_position_qty(step: dict, symbol: str | None = None) -> float:
    portfolio = dict(step.get("portfolio") or {})
    direct = safe_float(portfolio.get("position_qty"))
    if abs(direct) > 1e-8:
        return direct

    snapshot = dict(portfolio.get("snapshot_dict") or {})
    snap_qty = safe_float(snapshot.get("position_qty"))
    if abs(snap_qty) > 1e-8:
        return snap_qty

    positions = dict(snapshot.get("positions") or {})
    if symbol and symbol in positions:
        slot = dict(positions.get(symbol) or {})
        return safe_float(slot.get("qty"))
    if positions:
        first = next(iter(positions.values()))
        if isinstance(first, dict):
            return safe_float(first.get("qty"))
    return 0.0


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def annualized_sharpe(equity_curve: list[list[float]], bars_per_year: float = 365.0 * 24.0 * 4.0) -> float:
    if len(equity_curve) < 2:
        return 0.0
    returns: list[float] = []
    for idx in range(1, len(equity_curve)):
        prev = safe_float(equity_curve[idx - 1][1])
        cur = safe_float(equity_curve[idx][1])
        if prev > 0.0:
            returns.append(cur / prev - 1.0)
    if len(returns) < 2:
        return 0.0
    mean_ret = sum(returns) / len(returns)
    variance = sum((ret - mean_ret) ** 2 for ret in returns) / (len(returns) - 1)
    std = math.sqrt(max(variance, 0.0))
    if std <= 0.0:
        return 0.0
    return mean_ret / std * math.sqrt(bars_per_year)


def max_drawdown(equity_curve: list[list[float]]) -> float:
    peak = None
    worst = 0.0
    for _, equity in equity_curve:
        eq = safe_float(equity)
        peak = eq if peak is None else max(peak, eq)
        if peak and peak > 0.0:
            worst = min(worst, eq / peak - 1.0)
    return worst


def slice_equity_curve(equity_curve: list[list[float]], start_ts: int, end_ts: int) -> list[list[float]]:
    return [row for row in equity_curve if start_ts <= int(row[0]) <= end_ts]


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
