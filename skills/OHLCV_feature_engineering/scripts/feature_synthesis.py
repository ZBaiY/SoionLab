from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import (
    CORE_OHLCV_FIELDS,
    dump_json,
    family_from_type,
    feature_role_from_label,
    load_json,
    purpose_tag,
    yaml_dump,
    write_text,
)


def _base_spec(*, feature_id: str, rank: int, name: str, feature_type: str, description: str, expression: str,
               inputs: list[str], parameters: dict[str, Any], parsed: dict[str, Any], family: str,
               secondary_family: str | None = None, required_fields: list[str] | None = None,
               optional_fields: list[str] | None = None, reference_required: bool = False,
               hypothesis_id: str = "H01", feature_role: str | None = None,
               timing_extras: dict[str, Any] | None = None) -> dict[str, Any]:
    required_fields = required_fields or list(CORE_OHLCV_FIELDS)
    optional_fields = optional_fields or []
    window_values = [value for key, value in parameters.items() if key.endswith("_bars") and isinstance(value, int)]
    warmup = max(window_values or [2])
    return {
        "feature_id": feature_id,
        "candidate_rank": rank,
        "feature_name_template": name,
        "feature_type_label": feature_type,
        "family": family,
        "secondary_family": secondary_family,
        "feature_role": feature_role or feature_role_from_label(feature_type, family),
        "hypothesis_id": hypothesis_id,
        "description": description,
        "formula": {
            "expression": expression,
            "inputs": inputs,
            "parameters": {
                "window_bars": parameters.get("window_bars"),
                "slow_window_bars": parameters.get("slow_window_bars"),
                "fast_window_bars": parameters.get("fast_window_bars"),
                "normalization_window_bars": parameters.get("normalization_window_bars"),
                "other": {k: v for k, v in parameters.items() if k not in {
                    "window_bars", "slow_window_bars", "fast_window_bars", "normalization_window_bars"
                }},
            },
        },
        "timing_semantics": {
            "observation_interval": parsed["observation_interval"],
            "decision_timestamp_rule": "use data_ts <= visible_end_ts(step_ts)",
            "bar_state": "closed_only",
            "lookback_anchor": "trailing",
            **(timing_extras or {}),
        },
        "data_requirements": {
            "required_fields": required_fields,
            "optional_fields": optional_fields,
            "reference_symbol_required": reference_required,
        },
        "warmup": {
            "required_window_by_domain": {"ohlcv": warmup},
            "minimum_history_bars": warmup,
        },
        "missing_data_policy": {
            "insufficient_history": "return None until minimum_history_bars is satisfied",
            "missing_optional_field": "mark clarify if field is not guaranteed; do not silently substitute",
            "zero_division": "use epsilon guard and document neutral fallback",
        },
        "audit_status": "clarify",
        "audit_notes": ["Pending timing audit."],
    }


def synthesize(payload: dict[str, Any]) -> dict[str, Any]:
    parsed = payload["parsed_directions"]
    purpose = parsed.get("purpose_tag") or purpose_tag(parsed.get("goal_context"), parsed["raw_direction"])
    symbol = parsed["input_domain_scope"].get("primary_symbol", "{A}")
    ref_symbols = parsed["input_domain_scope"].get("reference_symbols") or ["{B}"]
    mode = parsed["signal_mode"]
    max_candidates = max(1, int(parsed.get("max_candidates", 4)))
    specs: list[dict[str, Any]] = []

    def add(spec: dict[str, Any]) -> None:
        if len(specs) < max_candidates:
            specs.append(spec)

    if mode == "reversal":
        add(_base_spec(
            feature_id="F01",
            rank=1,
            name=f"EXTENSION_{purpose}_{symbol}",
            feature_type="EXTENSION",
            family="price_path",
            description="Trailing-normalized same-bar directional extension.",
            expression="(close_t - open_t) / (mean(high_i - low_i, i=t-n+1..t) + eps)",
            inputs=["open_t", "close_t", "high_i", "low_i"],
            parameters={"window_bars": 10},
            parsed=parsed,
            hypothesis_id="H01",
        ))
        add(_base_spec(
            feature_id="F02",
            rank=2,
            name=f"CLOSE_LOCATION_WEAKNESS_{purpose}_{symbol}",
            feature_type="CLOSE_LOCATION_WEAKNESS",
            family="order_flow_proxy",
            secondary_family="price_path",
            description="Weak close-location within the same closed bar.",
            expression="1 - ((close_t - low_t) / (high_t - low_t + eps))",
            inputs=["close_t", "low_t", "high_t"],
            parameters={"window_bars": 1},
            parsed=parsed,
            hypothesis_id="H01",
            required_fields=["high", "low", "close"],
        ))
        add(_base_spec(
            feature_id="F03",
            rank=3,
            name=f"EXHAUSTION_COMPOSITE_{purpose}_{symbol}",
            feature_type="EXHAUSTION_COMPOSITE",
            family="cross_interaction",
            secondary_family="order_flow_proxy",
            description="Interaction between extension and close-location weakness.",
            expression="((close_t - open_t) / (mean(high_i - low_i, i=t-n+1..t) + eps)) * (1 - ((close_t - low_t) / (high_t - low_t + eps)))",
            inputs=["open_t", "close_t", "high_t", "low_t", "high_i", "low_i"],
            parameters={"window_bars": 10, "normalization_window_bars": 10},
            parsed=parsed,
            hypothesis_id="H02",
        ))
    elif mode == "trend":
        add(_base_spec(
            feature_id="F01",
            rank=1,
            name=f"LOG_RETURN_{purpose}_{symbol}",
            feature_type="LOG_RETURN",
            family="price_path",
            description="One-bar log return as the atomic drift basis.",
            expression="log(close_t / close_{t-1})",
            inputs=["close_t", "close_{t-1}"],
            parameters={"window_bars": 1},
            parsed=parsed,
            required_fields=["close"],
            feature_role="alpha_feature",
        ))
        add(_base_spec(
            feature_id="F02",
            rank=2,
            name=f"TREND_SLOPE_{purpose}_{symbol}",
            feature_type="TREND_SLOPE",
            family="price_path",
            description="OLS slope on trailing log price over a fixed window.",
            expression="ols_slope(log(close_i), i=t-n+1..t)",
            inputs=["close_i"],
            parameters={"window_bars": 8},
            parsed=parsed,
            required_fields=["close"],
            feature_role="regime_feature",
        ))
    elif mode == "volatility":
        add(_base_spec(
            feature_id="F01",
            rank=1,
            name=f"GARMAN_KLASS_{purpose}_{symbol}",
            feature_type="GARMAN_KLASS",
            family="volatility",
            description="Rolling Garman-Klass range-based volatility estimator.",
            expression="sqrt(mean(0.5*(log(high_i/low_i))^2 - (2*log(2)-1)*(log(close_i/open_i))^2, i=t-n+1..t))",
            inputs=["high_i", "low_i", "open_i", "close_i"],
            parameters={"window_bars": 8},
            parsed=parsed,
            required_fields=["high", "low", "open", "close"],
            feature_role="risk_feature",
        ))
        add(_base_spec(
            feature_id="F02",
            rank=2,
            name=f"INTRABAR_ASYMMETRY_{purpose}_{symbol}",
            feature_type="INTRABAR_ASYMMETRY",
            family="volatility",
            description="Signed body over full range as intrabar asymmetry proxy.",
            expression="(close_t - open_t) / max(high_t - low_t, eps)",
            inputs=["close_t", "open_t", "high_t", "low_t"],
            parameters={"window_bars": 1},
            parsed=parsed,
            required_fields=["open", "high", "low", "close"],
            feature_role="alpha_feature",
        ))
    elif mode == "volume":
        add(_base_spec(
            feature_id="F01",
            rank=1,
            name=f"REL_VOLUME_{purpose}_{symbol}",
            feature_type="REL_VOLUME",
            family="volume_liquidity",
            description="Current volume divided by trailing average volume.",
            expression="volume_t / (mean(volume_i, i=t-n+1..t) + eps)",
            inputs=["volume_t", "volume_i"],
            parameters={"window_bars": 20},
            parsed=parsed,
            required_fields=["volume"],
            feature_role="activity_feature",
        ))
        add(_base_spec(
            feature_id="F02",
            rank=2,
            name=f"VWAP_PROXY_PREMIUM_{purpose}_{symbol}",
            feature_type="VWAP_PROXY_PREMIUM",
            family="volume_liquidity",
            secondary_family="cross_interaction",
            description="VWAP proxy relative to close as a minimal price-adjusted liquidity-scale feature.",
            expression="(quote_asset_volume_t / max(volume_t, eps)) / max(close_t, eps) - 1",
            inputs=["quote_asset_volume_t", "volume_t", "close_t"],
            parameters={"window_bars": 1},
            parsed=parsed,
            required_fields=["quote_asset_volume", "volume", "close"],
            feature_role="alpha_feature",
        ))
    elif mode == "order_flow":
        add(_base_spec(
            feature_id="F01",
            rank=1,
            name=f"IMBALANCE_BASE_{purpose}_{symbol}",
            feature_type="IMBALANCE_BASE",
            family="order_flow_proxy",
            description="Normalized taker-buy aggressor imbalance using base-volume aggregates.",
            expression="(2 * taker_buy_base_asset_volume_t - volume_t) / max(volume_t, eps)",
            inputs=["taker_buy_base_asset_volume_t", "volume_t"],
            parameters={"window_bars": 1},
            parsed=parsed,
            required_fields=["volume", "taker_buy_base_asset_volume"],
            optional_fields=["taker_buy_base_asset_volume"],
            feature_role="alpha_feature",
        ))
        add(_base_spec(
            feature_id="F02",
            rank=2,
            name=f"IMBALANCE_X_RETURN_{purpose}_{symbol}",
            feature_type="IMBALANCE_X_RETURN",
            family="cross_interaction",
            secondary_family="order_flow_proxy",
            description="Interaction between base-volume imbalance and same-bar log return.",
            expression="((2 * taker_buy_base_asset_volume_t - volume_t) / max(volume_t, eps)) * log(close_t / close_{t-1})",
            inputs=["taker_buy_base_asset_volume_t", "volume_t", "close_t", "close_{t-1}"],
            parameters={"window_bars": 1},
            parsed=parsed,
            required_fields=["volume", "taker_buy_base_asset_volume", "close"],
            optional_fields=["taker_buy_base_asset_volume"],
            hypothesis_id="H02",
            feature_role="alpha_feature",
        ))
    elif mode == "inventory":
        timing_extras = {
            "decision_lag_bars": 1,
            "state_update_visibility": "update anchor_t and all derived inventory_state outputs only from the latest visible closed bar",
            "feature_bar_reference": "bar t = latest closed bar visible at step_ts",
            "execution_interpretation": "features computed from closed bar t may only inform decisions/execution for bar t+1 or later",
            "warmup_alignment": "warmup and recursive anchor seeding must end at the latest visible closed bar before the current decision step",
        }
        add(_base_spec(
            feature_id="F01",
            rank=1,
            name=f"INVENTORY_OVERHANG_{purpose}_{symbol}",
            feature_type="INVENTORY_OVERHANG",
            family="inventory_state",
            description="Log distance between current close and a quote-volume-refreshed recursive anchor price.",
            expression="log(close_t / anchor_t), where anchor_t = (1-refresh_t)*anchor_{t-1} + refresh_t*((high_t+low_t+close_t)/3)",
            inputs=["high_t", "low_t", "close_t", "quote_asset_volume_t", "quote_asset_volume_i"],
            parameters={"window_bars": 32, "refresh_cap": 0.20},
            parsed=parsed,
            required_fields=["high", "low", "close", "quote_asset_volume"],
            feature_role="regime_feature",
            timing_extras=timing_extras,
        ))
        add(_base_spec(
            feature_id="F02",
            rank=2,
            name=f"INVENTORY_PRESSURE_Z_{purpose}_{symbol}",
            feature_type="INVENTORY_PRESSURE_Z",
            family="inventory_state",
            description="Trailing z-score of inventory overhang to identify positioning extremity.",
            expression="(inventory_overhang_t - mean(inventory_overhang_i, i=t-m+1..t)) / (std(inventory_overhang_i, i=t-m+1..t) + eps)",
            inputs=["high_i", "low_i", "close_i", "quote_asset_volume_i"],
            parameters={"window_bars": 32, "normalization_window_bars": 64, "refresh_cap": 0.20},
            parsed=parsed,
            required_fields=["high", "low", "close", "quote_asset_volume"],
            feature_role="regime_feature",
            hypothesis_id="H02",
            timing_extras=timing_extras,
        ))
        add(_base_spec(
            feature_id="F03",
            rank=3,
            name=f"INVENTORY_OVERHANG_X_IMBALANCE_{purpose}_{symbol}",
            feature_type="INVENTORY_OVERHANG_X_IMBALANCE",
            family="inventory_state",
            secondary_family="order_flow_proxy",
            description="Inventory overhang conditioned by same-bar aggressor imbalance.",
            expression="inventory_overhang_t * ((2 * taker_buy_base_asset_volume_t - volume_t) / max(volume_t, eps))",
            inputs=["high_i", "low_i", "close_i", "quote_asset_volume_i", "taker_buy_base_asset_volume_t", "volume_t"],
            parameters={"window_bars": 32, "refresh_cap": 0.20},
            parsed=parsed,
            required_fields=["high", "low", "close", "quote_asset_volume", "taker_buy_base_asset_volume", "volume"],
            feature_role="alpha_feature",
            hypothesis_id="H03",
            timing_extras=timing_extras,
        ))
        add(_base_spec(
            feature_id="F04",
            rank=4,
            name=f"INVENTORY_OVERHANG_X_LOG_RETURN_{purpose}_{symbol}",
            feature_type="INVENTORY_OVERHANG_X_LOG_RETURN",
            family="inventory_state",
            secondary_family="price_path",
            description="Inventory overhang conditioned by same-bar signed return.",
            expression="inventory_overhang_t * log(close_t / close_{t-1})",
            inputs=["high_i", "low_i", "close_i", "quote_asset_volume_i", "close_t", "close_{t-1}"],
            parameters={"window_bars": 32, "refresh_cap": 0.20},
            parsed=parsed,
            required_fields=["high", "low", "close", "quote_asset_volume"],
            feature_role="alpha_feature",
            hypothesis_id="H03",
            timing_extras=timing_extras,
        ))
        add(_base_spec(
            feature_id="F05",
            rank=5,
            name=f"INVENTORY_OVERHANG_X_RANGE_EXPANSION_{purpose}_{symbol}",
            feature_type="INVENTORY_OVERHANG_X_RANGE_EXPANSION",
            family="inventory_state",
            secondary_family="volatility",
            description="Inventory overhang conditioned by trailing range expansion stress.",
            expression="inventory_overhang_t * ((high_t - low_t) / (mean(high_i - low_i, i=t-r+1..t) + eps))",
            inputs=["high_i", "low_i", "close_i", "quote_asset_volume_i"],
            parameters={"window_bars": 32, "normalization_window_bars": 20, "refresh_cap": 0.20},
            parsed=parsed,
            required_fields=["high", "low", "close", "quote_asset_volume"],
            feature_role="risk_feature",
            hypothesis_id="H03",
            timing_extras=timing_extras,
        ))
    elif mode == "activity":
        add(_base_spec(
            feature_id="F01",
            rank=1,
            name=f"AVG_TRADE_SIZE_{purpose}_{symbol}",
            feature_type="AVG_TRADE_SIZE",
            family="trade_activity",
            description="Average trade size within the bar.",
            expression="volume_t / max(number_of_trades_t, eps)",
            inputs=["volume_t", "number_of_trades_t"],
            parameters={"window_bars": 1},
            parsed=parsed,
            required_fields=["volume", "number_of_trades"],
            optional_fields=["number_of_trades"],
            feature_role="activity_feature",
        ))
        add(_base_spec(
            feature_id="F02",
            rank=2,
            name=f"TRADE_COUNT_REL_{purpose}_{symbol}",
            feature_type="TRADE_COUNT_REL",
            family="trade_activity",
            description="Current trade count relative to trailing average trade count.",
            expression="number_of_trades_t / (mean(number_of_trades_i, i=t-n+1..t) + eps)",
            inputs=["number_of_trades_t", "number_of_trades_i"],
            parameters={"window_bars": 20},
            parsed=parsed,
            required_fields=["number_of_trades"],
            optional_fields=["number_of_trades"],
            feature_role="activity_feature",
        ))
    elif mode == "time":
        add(_base_spec(
            feature_id="F01",
            rank=1,
            name=f"HOUR_SIN_{purpose}_{symbol}",
            feature_type="HOUR_SIN",
            family="time_structure",
            description="Sine encoding of hour-of-day from decision-time timestamp.",
            expression="sin(2*pi*hour(step_ts in market timezone)/24)",
            inputs=["step_ts"],
            parameters={"window_bars": 1},
            parsed=parsed,
            required_fields=[],
            feature_role="temporal_feature",
        ))
        add(_base_spec(
            feature_id="F02",
            rank=2,
            name=f"HOUR_COS_{purpose}_{symbol}",
            feature_type="HOUR_COS",
            family="time_structure",
            description="Cosine encoding of hour-of-day from decision-time timestamp.",
            expression="cos(2*pi*hour(step_ts in market timezone)/24)",
            inputs=["step_ts"],
            parameters={"window_bars": 1},
            parsed=parsed,
            required_fields=[],
            feature_role="temporal_feature",
        ))
    elif mode == "cross":
        ref = ref_symbols[0]
        add(_base_spec(
            feature_id="F01",
            rank=1,
            name=f"LOG_SPREAD_{purpose}_{symbol}^{ref}",
            feature_type="LOG_SPREAD",
            family="cross_interaction",
            description="Aligned log spread between primary and reference close.",
            expression="log(close_t_primary) - log(close_t_ref)",
            inputs=["close_t_primary", "close_t_ref"],
            parameters={"window_bars": 1},
            parsed=parsed,
            required_fields=["close"],
            reference_required=True,
        ))
        add(_base_spec(
            feature_id="F02",
            rank=2,
            name=f"SPREAD_ZSCORE_{purpose}_{symbol}^{ref}",
            feature_type="SPREAD_ZSCORE",
            family="cross_interaction",
            description="Trailing z-score of aligned primary/reference log spread.",
            expression="(spread_t - mean(spread_i, i=t-n+1..t)) / (std(spread_i, i=t-n+1..t) + eps)",
            inputs=["close_t_primary", "close_t_ref", "close_i_primary", "close_i_ref"],
            parameters={"window_bars": 30, "normalization_window_bars": 30},
            parsed=parsed,
            required_fields=["close"],
            reference_required=True,
        ))
    else:
        add(_base_spec(
            feature_id="F01",
            rank=1,
            name=f"LOG_RETURN_{purpose}_{symbol}",
            feature_type="LOG_RETURN",
            family="price_path",
            description="Trailing log return over a fixed bar horizon.",
            expression="log(close_t / close_{t-n})",
            inputs=["close_t", "close_{t-n}"],
            parameters={"window_bars": 5},
            parsed=parsed,
            required_fields=["close"],
        ))
        add(_base_spec(
            feature_id="F02",
            rank=2,
            name=f"PATH_EFFICIENCY_{purpose}_{symbol}",
            feature_type="PATH_EFFICIENCY",
            family="price_path",
            description="Directional efficiency over a trailing window.",
            expression="abs(close_t - close_{t-n}) / (sum(abs(close_i - close_{i-1}), i=t-n+1..t) + eps)",
            inputs=["close_t", "close_{t-n}", "close_i", "close_{i-1}"],
            parameters={"window_bars": 8},
            parsed=parsed,
            required_fields=["close"],
        ))

    for spec in specs:
        family, secondary = family_from_type(spec["feature_type_label"], spec["formula"]["expression"], spec["formula"]["inputs"])
        spec["family"] = family
        spec["secondary_family"] = spec["secondary_family"] or secondary
        spec["feature_role"] = feature_role_from_label(spec["feature_type_label"], spec["family"])

    return {
        "parsed_directions": parsed,
        "hypotheses": payload["hypotheses"],
        "feature_specifications": specs,
    }


def run(input_path: Path, output_path: Path, yaml_output: Path | None = None) -> dict[str, Any]:
    payload = load_json(input_path)
    result = synthesize(payload)
    dump_json(output_path, result)
    if yaml_output is not None:
        write_text(yaml_output, yaml_dump(result) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--yaml-output")
    args = parser.parse_args()
    run(Path(args.input), Path(args.output), Path(args.yaml_output) if args.yaml_output else None)


if __name__ == "__main__":
    main()
