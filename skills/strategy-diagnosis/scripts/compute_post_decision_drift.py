from __future__ import annotations

import argparse
import json
from pathlib import Path

from common import extract_position_qty, iter_trace_steps, safe_float, write_csv


def parse_horizons(raw: str) -> list[int]:
    return [int(part) for part in str(raw).split(",") if part.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute forward drift after decisions from trace features.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--horizons", default="1,3,6,12,24")
    parser.add_argument("--buy-threshold", type=float, default=0.0)
    parser.add_argument("--sell-threshold", type=float, default=0.0)
    parser.add_argument("--out-csv", required=True)
    parser.add_argument("--out-json", default=None)
    args = parser.parse_args()

    steps = list(iter_trace_steps(Path(args.repo_root), args.run_id))
    closes: list[float] = []
    for step in steps:
        market_snapshots = dict(step.get("market_snapshots") or {})
        ohlcv = dict(market_snapshots.get("ohlcv") or {})
        symbol_snap = dict(ohlcv.get(args.symbol) or {})
        numeric = dict(symbol_snap.get("numeric") or {})
        closes.append(safe_float(numeric.get("close")))

    horizons = parse_horizons(args.horizons)
    rows: list[dict[str, object]] = []
    for idx, step in enumerate(steps):
        base_close = closes[idx]
        if base_close <= 0.0:
            continue
        decision_score = safe_float(step.get("decision_score"))
        fills = list(step.get("fills") or [])
        has_buy_fill = any(safe_float(fill.get("filled_qty")) > 0.0 for fill in fills if isinstance(fill, dict))
        has_sell_fill = any(safe_float(fill.get("filled_qty")) < 0.0 for fill in fills if isinstance(fill, dict))
        qty = extract_position_qty(step, args.symbol)
        if decision_score > float(args.buy_threshold):
            event_type = "buy_filled" if has_buy_fill else "buy_blocked"
        elif decision_score < -float(args.sell_threshold):
            event_type = "sell_filled" if has_sell_fill else "sell_blocked"
        elif abs(qty) > 1e-8:
            event_type = "hold_in_position"
        else:
            event_type = "hold_flat"
        for horizon in horizons:
            j = idx + horizon
            if j >= len(closes) or closes[j] <= 0.0:
                continue
            rows.append(
                {
                    "run_id": args.run_id,
                    "ts_ms": int(step.get("ts_ms") or 0),
                    "event_type": event_type,
                    "horizon_bars": horizon,
                    "forward_return": closes[j] / base_close - 1.0,
                    "decision_score": decision_score,
                    "fills_count": len(fills),
                    "position_qty": qty,
                }
            )

    write_csv(Path(args.out_csv), rows)
    if args.out_json:
        Path(args.out_json).write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
