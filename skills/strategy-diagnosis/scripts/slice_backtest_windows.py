from __future__ import annotations

import argparse
import json
from pathlib import Path

from common import annualized_sharpe, load_report, max_drawdown, safe_float, slice_equity_curve, write_csv


def parse_windows(raw: str) -> list[tuple[str, int, int]]:
    windows: list[tuple[str, int, int]] = []
    for part in str(raw).split(","):
        label, start_ts, end_ts = part.split(":")
        windows.append((label, int(start_ts), int(end_ts)))
    return windows


def main() -> None:
    parser = argparse.ArgumentParser(description="Slice a full-run report into calendar windows.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--windows", required=True, help="label:start:end,label:start:end")
    parser.add_argument("--out-csv", required=True)
    parser.add_argument("--out-json", default=None)
    args = parser.parse_args()

    report = load_report(Path(args.repo_root), args.run_id)
    equity_curve = list(report.get("equity_curve") or [])
    rows: list[dict[str, object]] = []
    for label, start_ts, end_ts in parse_windows(args.windows):
        window_curve = slice_equity_curve(equity_curve, start_ts, end_ts)
        if len(window_curve) < 2:
            continue
        start_eq = safe_float(window_curve[0][1])
        end_eq = safe_float(window_curve[-1][1])
        rows.append(
            {
                "run_id": args.run_id,
                "label": label,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "total_return": (end_eq / start_eq - 1.0) if start_eq > 0.0 else 0.0,
                "sharpe": annualized_sharpe(window_curve),
                "max_drawdown": max_drawdown(window_curve),
                "points": len(window_curve),
            }
        )
    write_csv(Path(args.out_csv), rows)
    if args.out_json:
        Path(args.out_json).write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
