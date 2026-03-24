from __future__ import annotations

import argparse
import json
from pathlib import Path

from common import iter_trace_steps, parse_feature_key_map, safe_float, write_csv


def classify_step(
    step: dict,
    *,
    trend_key: str | None,
    bullish_key: str | None,
    bearish_key: str | None,
    oscillator_key: str | None,
    trend_high: float,
    trend_low: float,
    oscillator_high: float,
    oscillator_low: float,
) -> str:
    features = dict(step.get("features") or {})
    trend_strength = safe_float(features.get(trend_key)) if trend_key else 0.0
    bullish = safe_float(features.get(bullish_key)) if bullish_key else 0.0
    bearish = safe_float(features.get(bearish_key)) if bearish_key else 0.0
    oscillator = safe_float(features.get(oscillator_key)) if oscillator_key else 50.0

    if trend_strength >= trend_high and bullish > bearish:
        return "trend_up"
    if trend_strength >= trend_high and bearish > bullish:
        return "trend_down"
    if trend_strength < trend_low and oscillator_low < oscillator < oscillator_high:
        return "sideways_low_vol"
    if trend_strength < trend_high:
        return "choppy_high_vol"
    if oscillator >= oscillator_high and bullish > bearish:
        return "breakout_post_breakout"
    if oscillator <= oscillator_low and bearish > bullish:
        return "liquidation_panic_reversal"
    return "unclassified"


def main() -> None:
    parser = argparse.ArgumentParser(description="Classify per-step regimes from a trace log.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--feature-keys", default="", help="trend=KEY,bullish=KEY,bearish=KEY,oscillator=KEY")
    parser.add_argument("--trend-high", type=float, default=30.0)
    parser.add_argument("--trend-low", type=float, default=20.0)
    parser.add_argument("--oscillator-high", type=float, default=70.0)
    parser.add_argument("--oscillator-low", type=float, default=30.0)
    parser.add_argument("--out-csv", required=True)
    parser.add_argument("--out-json", default=None)
    args = parser.parse_args()

    key_map = parse_feature_key_map(args.feature_keys)
    rows: list[dict[str, object]] = []
    for step in iter_trace_steps(Path(args.repo_root), args.run_id):
        features = dict(step.get("features") or {})
        trend_key = key_map.get("trend", "ADX_DECISION_BTCUSDT")
        bullish_key = key_map.get("bullish", "ADX-DI-PLUS_DECISION_BTCUSDT")
        bearish_key = key_map.get("bearish", "ADX-DI-MINUS_DECISION_BTCUSDT")
        oscillator_key = key_map.get("oscillator", "RSI_DECISION_BTCUSDT")
        rows.append(
            {
                "run_id": args.run_id,
                "ts_ms": int(step.get("ts_ms") or 0),
                "regime": classify_step(
                    step,
                    trend_key=trend_key,
                    bullish_key=bullish_key,
                    bearish_key=bearish_key,
                    oscillator_key=oscillator_key,
                    trend_high=float(args.trend_high),
                    trend_low=float(args.trend_low),
                    oscillator_high=float(args.oscillator_high),
                    oscillator_low=float(args.oscillator_low),
                ),
                "trend_value": safe_float(features.get(trend_key)),
                "bullish_value": safe_float(features.get(bullish_key)),
                "bearish_value": safe_float(features.get(bearish_key)),
                "oscillator_value": safe_float(features.get(oscillator_key)),
                "decision_score": safe_float(step.get("decision_score")),
                "target_position": safe_float(step.get("target_position")),
            }
        )

    write_csv(Path(args.out_csv), rows)
    if args.out_json:
        Path(args.out_json).write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
