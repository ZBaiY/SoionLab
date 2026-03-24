from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

from common import load_report, safe_float, write_csv


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fp:
        return list(csv.DictReader(fp))


def main() -> None:
    parser = argparse.ArgumentParser(description="Join regime labels with step returns into a regime matrix.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--regime-csv", required=True)
    parser.add_argument("--out-csv", required=True)
    parser.add_argument("--out-json", default=None)
    args = parser.parse_args()

    labels = load_csv(Path(args.regime_csv))
    report = load_report(Path(args.repo_root), args.run_id)
    equity_curve = list(report.get("equity_curve") or [])
    eq_by_ts = {int(ts): safe_float(eq) for ts, eq in equity_curve}

    grouped: dict[str, dict[str, float]] = defaultdict(lambda: {"steps": 0.0, "sum_return": 0.0, "wins": 0.0, "losses": 0.0})
    prev_eq = None
    prev_ts = None
    for row in labels:
        ts = int(row["ts_ms"])
        eq = eq_by_ts.get(ts)
        if eq is None:
            continue
        if prev_eq is not None and prev_eq > 0.0 and prev_ts is not None and ts > prev_ts:
            ret = eq / prev_eq - 1.0
            regime = row["regime"]
            grouped[regime]["steps"] += 1.0
            grouped[regime]["sum_return"] += ret
            if ret > 0.0:
                grouped[regime]["wins"] += 1.0
            elif ret < 0.0:
                grouped[regime]["losses"] += 1.0
        prev_eq = eq
        prev_ts = ts

    rows: list[dict[str, object]] = []
    for regime, agg in sorted(grouped.items()):
        steps = agg["steps"]
        rows.append(
            {
                "run_id": args.run_id,
                "regime": regime,
                "steps": int(steps),
                "mean_step_return": (agg["sum_return"] / steps) if steps else 0.0,
                "positive_step_ratio": (agg["wins"] / steps) if steps else 0.0,
                "negative_step_ratio": (agg["losses"] / steps) if steps else 0.0,
            }
        )

    write_csv(Path(args.out_csv), rows)
    if args.out_json:
        Path(args.out_json).write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
