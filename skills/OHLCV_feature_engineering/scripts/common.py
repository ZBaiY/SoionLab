from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[3]
LIBRARY_ROOT = REPO_ROOT / "research_library/ohlcv_feature_engineering"
CORE_OHLCV_FIELDS = ["open", "high", "low", "close", "volume"]
OPTIONAL_AUX_FIELDS = [
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "open_time",
    "close_time",
]
QUALITY_DIMENSIONS = [
    "causal_safety",
    "decision_time_clarity",
    "interpretability",
    "implementation_complexity",
    "expected_incremental_information",
    "data_efficiency",
    "robustness_to_regime_shift",
    "overfit_risk",
]
FAMILY_ORDER = [
    "price_path",
    "volatility",
    "volume_liquidity",
    "order_flow_proxy",
    "inventory_state",
    "trade_activity",
    "time_structure",
    "cross_interaction",
]
ROLE_ORDER = [
    "alpha_feature",
    "regime_feature",
    "risk_feature",
    "activity_feature",
    "temporal_feature",
]
MECHANISM_ORDER = [
    "price_direction",
    "flow_pressure",
    "execution_displacement",
    "inventory_state",
    "regime_state",
    "risk_state",
    "participation_intensity",
    "trade_fragmentation",
    "temporal_phase",
    "interaction",
]


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def dump_json(path: Path, payload: Any) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    ensure_parent(path)
    path.write_text(text, encoding="utf-8")


def read_template(name: str) -> str:
    return (Path(__file__).resolve().parent / "templates" / name).read_text(encoding="utf-8")


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def slugify(text: str, *, max_len: int = 64) -> str:
    value = normalize_text(text).lower()
    value = re.sub(r"[^a-z0-9]+", "_", value).strip("_")
    return value[:max_len] or "record"


def stable_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_date_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def yaml_dump(data: Any, indent: int = 0) -> str:
    prefix = "  " * indent
    if isinstance(data, dict):
        lines: list[str] = []
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                lines.append(f"{prefix}{key}:")
                lines.append(yaml_dump(value, indent + 1))
            else:
                lines.append(f"{prefix}{key}: {yaml_scalar(value)}")
        return "\n".join(lines)
    if isinstance(data, list):
        lines = []
        for item in data:
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}-")
                lines.append(yaml_dump(item, indent + 1))
            else:
                lines.append(f"{prefix}- {yaml_scalar(item)}")
        return "\n".join(lines)
    return f"{prefix}{yaml_scalar(data)}"


def yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(str(value))


def purpose_tag(goal_context: str | None, direction_text: str) -> str:
    text = f"{goal_context or ''} {direction_text}".lower()
    if any(token in text for token in ("risk", "sizing", "stop", "exposure")):
        return "RISK"
    if any(token in text for token in ("decision", "filter", "trigger", "entry", "exit")):
        return "DECISION"
    return "MODEL"


def signal_mode(direction_text: str, goal_context: str | None = None, family_hint: str | None = None) -> str:
    if isinstance(family_hint, str) and family_hint.strip():
        normalized = family_hint.strip().lower()
        mapping = {
            "price_path": "trend",
            "volatility": "volatility",
            "volume_liquidity": "volume",
            "order_flow_proxy": "order_flow",
            "inventory_state": "inventory",
            "trade_activity": "activity",
            "time_structure": "time",
            "cross_interaction": "cross",
        }
        if normalized in mapping:
            return mapping[normalized]

    text = f"{direction_text} {goal_context or ''}".lower()
    if any(token in text for token in ("order flow", "aggressor", "imbalance", "taker_buy", "taker buy", "signed volume")):
        return "order_flow"
    if any(token in text for token in ("inventory", "cost-basis", "cost basis", "anchor price", "crowd cost", "overhang", "positioning state")):
        return "inventory"
    if any(token in text for token in ("number_of_trades", "trade count", "avg trade size", "trade intensity", "activity")):
        return "activity"
    if any(token in text for token in ("price path", "drift", "geometry", "trend slope", "momentum", "cumulative return")):
        return "trend"
    if any(token in text for token in ("exhaust", "reversal", "fade", "weak follow", "follow-through")):
        return "reversal"
    if any(token in text for token in ("trend", "momentum", "continuation", "breakout", "persistence")):
        return "trend"
    if any(token in text for token in ("volatility", "compression", "expansion", "range")):
        return "volatility"
    if any(token in text for token in ("volume", "liquidity", "participation")):
        return "volume"
    if any(token in text for token in ("trade count", "activity", "taker")):
        return "activity"
    if any(token in text for token in ("hour", "session", "day-of-week", "time of day", "seasonality")):
        return "time"
    if any(token in text for token in ("spread", "relative", "versus", "vs ", "cross")):
        return "cross"
    return "price_path"


def detect_horizon_bars(direction_text: str) -> int | None:
    match = re.search(r"(\d+)\s*(bar|bars|candle|candles)", direction_text.lower())
    if match:
        return int(match.group(1))
    text = direction_text.lower()
    if "intrabar" in text:
        return 1
    if "short" in text:
        return 3
    if "medium" in text:
        return 12
    if "slow" in text or "long" in text:
        return 24
    return None


def allowed_fields(available_fields: list[str] | None) -> set[str]:
    base = set(CORE_OHLCV_FIELDS) | set(OPTIONAL_AUX_FIELDS)
    if available_fields:
        base |= set(available_fields)
    return base


def family_from_type(feature_type: str, expression: str = "", inputs: list[str] | None = None) -> tuple[str, str | None]:
    text = f"{feature_type} {expression} {' '.join(inputs or [])}".lower()
    if any(token in text for token in ("imbalance", "buy_share", "taker_buy", "signed_volume")):
        return "order_flow_proxy", None
    if any(token in text for token in ("spread", "zscore", "ref_", "ref)", "reference")):
        return "cross_interaction", None
    if any(token in text for token in ("inventory_overhang", "inventory_pressure_z", "inventory_anchor", "cost_basis", "cost_basis_proxy", "anchor_price")):
        return "inventory_state", None
    if any(token in text for token in ("trade_count", "number_of_trades", "avg_trade_size", "trade_intensity")):
        return "trade_activity", None
    if any(token in text for token in ("hour_of_day", "day_of_week", "session", "hour_sin", "hour_cos", "step_ts")):
        return "time_structure", None
    if any(token in text for token in ("volume", "dollar_volume", "participation")):
        return "volume_liquidity", None
    if any(token in text for token in ("atr", "true_range", "vol_ratio", "variance", "std", "parkinson", "garman_klass", "squared_return", "intrabar_asymmetry")):
        return "volatility", None
    if any(token in text for token in ("close_location", "wick", "pressure")):
        return "order_flow_proxy", "price_path"
    return "price_path", None


def feature_role_from_label(label: str, family: str | None = None) -> str:
    text = label.lower()
    if any(token in text for token in ("inventory_overhang_x_range_expansion",)):
        return "risk_feature"
    if any(token in text for token in ("inventory_overhang_x_imbalance", "inventory_overhang_x_log_return")):
        return "alpha_feature"
    if any(token in text for token in ("inventory_overhang", "inventory_pressure_z", "inventory_anchor")):
        return "regime_feature"
    if any(token in text for token in ("hour_sin", "hour_cos", "day_of_week", "session_phase")):
        return "temporal_feature"
    if any(token in text for token in ("garman_klass", "parkinson", "squared_return", "vol_ratio", "realized_vol")):
        return "risk_feature"
    if any(token in text for token in ("intrabar_asymmetry", "log_return", "imbalance_base", "imbalance_x_return", "vwap_proxy_premium")):
        return "alpha_feature"
    if any(token in text for token in ("trend_slope", "path_efficiency", "cumulative_return", "trend_regime")):
        return "regime_feature"
    if any(token in text for token in ("rel_volume", "avg_trade_size", "trade_count_rel", "trade_intensity")):
        return "activity_feature"
    if family == "trade_activity":
        return "activity_feature"
    if family == "time_structure":
        return "temporal_feature"
    if family == "inventory_state":
        return "regime_feature"
    if family == "volatility":
        return "risk_feature"
    return "alpha_feature"


def mechanism_class_from_label(label: str, family: str | None = None) -> str:
    text = label.lower()
    if any(token in text for token in ("inventory_overhang_x_",)):
        return "interaction"
    if any(token in text for token in ("inventory_overhang", "inventory_pressure_z", "inventory_anchor")):
        return "inventory_state"
    if any(token in text for token in ("log_return", "intrabar_asymmetry", "close_location", "extension")):
        return "price_direction"
    if any(token in text for token in ("imbalance", "buy_share", "signed_volume", "taker_buy")):
        return "flow_pressure"
    if any(token in text for token in ("vwap_proxy_premium", "dollar_volume", "quote_asset_volume")):
        return "execution_displacement"
    if any(token in text for token in ("trend_slope", "path_efficiency", "cumulative_return", "trend_regime")):
        return "regime_state"
    if any(token in text for token in ("garman_klass", "parkinson", "squared_return", "vol_ratio", "realized_vol")):
        return "risk_state"
    if any(token in text for token in ("rel_volume", "trade_count_rel", "trade_intensity")):
        return "participation_intensity"
    if any(token in text for token in ("avg_trade_size", "number_of_trades")):
        return "trade_fragmentation"
    if any(token in text for token in ("hour_sin", "hour_cos", "day_of_week", "session_phase")):
        return "temporal_phase"
    if family == "time_structure":
        return "temporal_phase"
    if family == "inventory_state":
        return "inventory_state"
    if family == "volatility":
        return "risk_state"
    if family == "trade_activity":
        return "participation_intensity"
    return "interaction"


def score_average(scores: dict[str, int]) -> float:
    return sum(scores.values()) / float(len(scores))


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    counter = 2
    while True:
        candidate = parent / f"{stem}__v{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def load_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def write_table(df: pd.DataFrame, path: Path) -> None:
    ensure_parent(path)
    if path.suffix.lower() == ".parquet":
        df.to_parquet(path, index=False)
        return
    df.to_csv(path, index=False)


def parse_features_arg(features: str | list[str] | None, df: pd.DataFrame | None = None) -> list[str]:
    if isinstance(features, list):
        return [item for item in features if item]
    if isinstance(features, str) and features.strip():
        return [item.strip() for item in features.split(",") if item.strip()]
    if df is None:
        return []
    default_exclude = {"timestamp", "data_ts", "symbol", "close"}
    return [column for column in df.columns if column not in default_exclude and not column.startswith("target_")]
