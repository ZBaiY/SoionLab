from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import CORE_OHLCV_FIELDS, detect_horizon_bars, dump_json, load_json, normalize_text, purpose_tag, signal_mode, stable_hash


def build_hypotheses(parsed: dict[str, Any]) -> list[dict[str, Any]]:
    mode = parsed["signal_mode"]
    hypotheses: list[dict[str, Any]] = []
    if mode == "reversal":
        hypotheses.append({
            "hypothesis_id": "H01",
            "statement": "Large directional candles that close with weak bar-quality are more likely to reflect exhaustion than durable continuation.",
            "observable_proxy": "Trailing-normalized bar extension and close-location weakness.",
            "causal_rationale": "Both extension and close-location can be measured on the last closed bar without future information.",
            "expected_use": "Short-horizon reversal or exhaustion filter.",
        })
        hypotheses.append({
            "hypothesis_id": "H02",
            "statement": "Exhaustion is stronger when recent price extension and weak bar-quality appear together rather than alone.",
            "observable_proxy": "Interaction between normalized candle extension and close-location weakness.",
            "causal_rationale": "A causal interaction can be formed from same-bar and trailing-bar OHLCV statistics.",
            "expected_use": "Composite confirmation feature.",
        })
    elif mode == "trend":
        hypotheses.append({
            "hypothesis_id": "H01",
            "statement": "Persistent directional movement over trailing bars carries more information than a single-bar move.",
            "observable_proxy": "Trailing return efficiency and volatility-normalized return.",
            "causal_rationale": "Trailing returns and efficiency use only closed historical bars.",
            "expected_use": "Trend strength or continuation signal.",
        })
    elif mode == "volatility":
        hypotheses.append({
            "hypothesis_id": "H01",
            "statement": "Volatility regimes can be identified by comparing current range behavior to trailing range behavior.",
            "observable_proxy": "True-range expansion and short/long volatility ratio.",
            "causal_rationale": "All required inputs are trailing OHLCV bars.",
            "expected_use": "Regime filter or volatility gate.",
        })
    elif mode == "volume":
        hypotheses.append({
            "hypothesis_id": "H01",
            "statement": "Participation shocks are more informative after normalizing raw volume by trailing baseline activity.",
            "observable_proxy": "Relative volume and trailing-normalized dollar volume.",
            "causal_rationale": "Volume normalization can be computed on trailing closed bars only.",
            "expected_use": "Participation filter or confirmation signal.",
        })
    elif mode == "order_flow":
        hypotheses.append({
            "hypothesis_id": "H01",
            "statement": "Taker-share imbalance is the cleanest OHLCV-level proxy for aggressor pressure.",
            "observable_proxy": "Taker buy share and normalized aggressor imbalance from bar aggregates.",
            "causal_rationale": "The taker fields are bar-closed aggregates and can be normalized causally within the same bar.",
            "expected_use": "Pressure, imbalance, or directional confirmation feature.",
        })
        hypotheses.append({
            "hypothesis_id": "H02",
            "statement": "Order-flow proxy is most useful when interacted with contemporaneous price or volatility state rather than used only in raw form.",
            "observable_proxy": "Imbalance multiplied by same-bar return or trailing volatility state.",
            "causal_rationale": "Same-bar and trailing-bar interactions remain causal under closed-bar semantics.",
            "expected_use": "Interaction feature for decision or model context.",
        })
    elif mode == "activity":
        hypotheses.append({
            "hypothesis_id": "H01",
            "statement": "Trade fragmentation and average trade size describe activity structure that is related to, but not identical with, raw volume.",
            "observable_proxy": "Normalized trade count and average trade size from bar-level aggregates.",
            "causal_rationale": "Both inputs are bar-level aggregates visible on bar close.",
            "expected_use": "Activity filter or participation structure feature.",
        })
    elif mode == "time":
        hypotheses.append({
            "hypothesis_id": "H01",
            "statement": "Predictive structure may vary systematically across recurring time buckets.",
            "observable_proxy": "Hour-of-day or session-position encodings.",
            "causal_rationale": "Time bucket encodings are known at decision time without future data.",
            "expected_use": "Seasonality or session filter.",
        })
    elif mode == "cross":
        hypotheses.append({
            "hypothesis_id": "H01",
            "statement": "Relative dislocations between aligned OHLCV series can proxy spread compression or divergence.",
            "observable_proxy": "Log spread and trailing spread z-score.",
            "causal_rationale": "Both series can be aligned on closed bars at the same decision timestamp.",
            "expected_use": "Relative-value or cross-symbol state feature.",
        })
    else:
        hypotheses.append({
            "hypothesis_id": "H01",
            "statement": "Directional information is better represented by normalized trailing path behavior than by raw price level.",
            "observable_proxy": "Trailing return and path-efficiency style price-path measures.",
            "causal_rationale": "Trailing OHLCV history is available at decision time.",
            "expected_use": "General price-path feature.",
        })
    return hypotheses


def parse_direction(payload: dict[str, Any]) -> dict[str, Any]:
    direction_text = normalize_text(str(payload["direction_text"]))
    goal_context = payload.get("goal_context")
    family_hint = payload.get("family_hint")
    mode = signal_mode(direction_text, goal_context, family_hint)
    purpose = purpose_tag(goal_context, direction_text)
    horizon = detect_horizon_bars(direction_text)
    available = payload.get("available_fields") or CORE_OHLCV_FIELDS
    primary_symbol = (payload.get("symbol_scope") or {}).get("primary_symbol", "PRIMARY")
    reference_symbols = list((payload.get("symbol_scope") or {}).get("reference_symbols") or [])
    constraints = list(payload.get("constraints") or [])
    parsed = {
        "direction_id": f"dir_{stable_hash({'direction_text': direction_text, 'goal_context': goal_context})[:10]}",
        "raw_direction": direction_text,
        "interpreted_objective": {
            "reversal": "Identify exhaustion or reversal pressure from OHLCV-only closed-bar structure.",
            "trend": "Measure trailing trend persistence from OHLCV-only closed-bar structure.",
            "volatility": "Measure volatility regime state from trailing OHLCV windows.",
            "volume": "Measure participation or liquidity pressure from volume-normalized OHLCV.",
            "order_flow": "Measure taker-share and aggressor-pressure proxy from OHLCV bar aggregates.",
            "activity": "Measure bar-level activity using OHLCV aux fields when available.",
            "time": "Measure recurring temporal structure available at decision time.",
            "cross": "Measure aligned cross-symbol dislocation from OHLCV-only bars.",
            "price_path": "Measure causal price-path structure from trailing OHLCV bars.",
        }[mode],
        "market_behavior": mode,
        "signal_mode": mode,
        "purpose_tag": purpose,
        "expected_horizon_bars": horizon,
        "family_hint": family_hint,
        "input_domain_scope": {
            "primary": ["ohlcv"],
            "primary_symbol": primary_symbol,
            "optional_aux_fields": [field for field in available if field not in CORE_OHLCV_FIELDS],
            "reference_symbols": reference_symbols,
        },
        "explicit_constraints": constraints,
        "assumptions": [
            "Feature must be computable from closed OHLCV bars only.",
            "Observation interval is semantic time; ingestion cadence is ignored.",
            "Feature count must remain bounded and interpretable.",
        ],
        "observation_interval": payload.get("observation_interval", "15m"),
        "available_fields": available,
        "max_candidates": int(payload.get("max_candidates", 4)),
    }
    return {
        "parsed_directions": parsed,
        "hypotheses": build_hypotheses(parsed),
    }


def run(input_path: Path, output_path: Path) -> dict[str, Any]:
    payload = load_json(input_path)
    result = parse_direction(payload)
    dump_json(output_path, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    run(Path(args.input), Path(args.output))


if __name__ == "__main__":
    main()
