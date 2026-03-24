from __future__ import annotations

import argparse
import json
from pathlib import Path

from common import extract_position_qty, iter_trace_steps, load_report, safe_float, write_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize strategy behavior from trace position state.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--out-csv", required=True)
    parser.add_argument("--out-json", default=None)
    args = parser.parse_args()

    report = load_report(Path(args.repo_root), args.run_id)
    round_trips = list(report.get("trade_round_trips") or [])
    rows: list[dict[str, object]] = []
    in_market_steps = 0
    total_steps = 0
    for step in iter_trace_steps(Path(args.repo_root), args.run_id):
        total_steps += 1
        qty = extract_position_qty(step, args.symbol)
        if abs(qty) > 1e-8:
            in_market_steps += 1

    hold_lengths = [int(trade.get("holding_steps") or 0) for trade in round_trips]
    realized = [safe_float(trade.get("realized_pnl")) for trade in round_trips]
    wins = [pnl for pnl in realized if pnl > 0.0]
    losses = [pnl for pnl in realized if pnl < 0.0]

    rows.append(
        {
            "run_id": args.run_id,
            "entry_count": len(round_trips),
            "time_in_market_ratio": in_market_steps / total_steps if total_steps else 0.0,
            "mean_holding_steps": (sum(hold_lengths) / len(hold_lengths)) if hold_lengths else 0.0,
            "max_holding_steps": max(hold_lengths) if hold_lengths else 0,
            "completed_holds": len(hold_lengths),
            "total_steps": total_steps,
            "win_rate": (len(wins) / len(round_trips)) if round_trips else 0.0,
            "avg_realized_pnl": (sum(realized) / len(realized)) if realized else 0.0,
            "avg_win_pnl": (sum(wins) / len(wins)) if wins else 0.0,
            "avg_loss_pnl": (sum(losses) / len(losses)) if losses else 0.0,
            "data_source": "report.trade_round_trips+trace.position_state",
        }
    )

    write_csv(Path(args.out_csv), rows)
    if args.out_json:
        Path(args.out_json).write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
