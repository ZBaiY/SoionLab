from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any

from quant_engine.data.ohlcv.snapshot import OHLCVSnapshot
from quant_engine.models.registry import build_model
from quant_engine.strategy.registry import STRATEGY_REGISTRY
from quant_engine.utils.logger import to_jsonable


_SYMBOL_POOL = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]


def _collect_placeholders(obj: Any) -> set[str]:
    placeholders: set[str] = set()
    if isinstance(obj, str):
        placeholders.update(re.findall(r"{([^{}]+)}", obj))
        return placeholders
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(key, str):
                placeholders.update(re.findall(r"{([^{}]+)}", key))
            placeholders.update(_collect_placeholders(value))
        return placeholders
    if isinstance(obj, (list, tuple, set)):
        for item in obj:
            placeholders.update(_collect_placeholders(item))
        return placeholders
    return placeholders


def _placeholder_value(name: str, symbol_idx: int) -> str:
    upper = name.upper()
    lower = name.lower()
    if upper in {"A", "B", "C", "D"}:
        return _SYMBOL_POOL[min(symbol_idx, len(_SYMBOL_POOL) - 1)]
    if "rolling" in lower or "roll" in lower:
        return "5"
    if "window" in lower or "lookback" in lower or "period" in lower:
        return "14"
    if "symbol" in lower or "asset" in lower or "ticker" in lower:
        return _SYMBOL_POOL[0]
    return "1"


def _symbol_map_for_strategy(strategy_cls: type) -> dict[str, str]:
    placeholders: set[str] = set()
    for obj in (
        strategy_cls.UNIVERSE_TEMPLATE,
        strategy_cls.DATA,
        strategy_cls.FEATURES_USER,
        strategy_cls.MODEL_CFG,
        strategy_cls.DECISION_CFG,
        strategy_cls.RISK_CFG,
        strategy_cls.EXECUTION_CFG,
        strategy_cls.PORTFOLIO_CFG,
        strategy_cls.PRESETS,
    ):
        placeholders.update(_collect_placeholders(obj))

    symbol_map: dict[str, str] = {}
    symbol_idx = 0
    for name in sorted(placeholders):
        value = _placeholder_value(name, symbol_idx)
        symbol_map[name] = value
        if value in _SYMBOL_POOL and name.upper() in {"A", "B", "C", "D"}:
            symbol_idx += 1
    return symbol_map


def _strategy_configs() -> list:
    cfgs = []
    for strategy_cls in STRATEGY_REGISTRY.values():
        symbols = _symbol_map_for_strategy(strategy_cls)
        cfgs.append(strategy_cls.standardize(symbols=symbols))
    return cfgs


def _stable_feature_value(name: str) -> float:
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()
    seed = int(digest[:8], 16)
    return (seed % 2000) / 1000.0 - 1.0


def _assert_finite(value: float) -> None:
    assert isinstance(value, (float, int))
    assert math.isfinite(float(value))


def test_models_contracts() -> None:
    covered: set[tuple[str, str]] = set()

    for cfg in _strategy_configs():
        if cfg.model is None:
            continue
        model_cfg = cfg.model
        model_type = str(model_cfg["type"])
        params = dict(model_cfg.get("params", {}))
        symbol = str(cfg.symbol or _SYMBOL_POOL[0])
        model = build_model(model_type, symbol=symbol, **params)

        feature_names = [str(item["name"]) for item in cfg.features_user]
        features = {name: _stable_feature_value(name) for name in feature_names}
        model.bind_feature_index(feature_names)
        model.validate_feature_types({str(item["type"]) for item in cfg.features_user})

        pred1 = model.predict(features)
        pred2 = model.predict(features)
        if isinstance(pred1, (float, int)):
            _assert_finite(pred1)
        if isinstance(pred2, (float, int)):
            _assert_finite(pred2)
        assert pred1 == pred2
        json.dumps(to_jsonable(pred1))

        snapshot = OHLCVSnapshot.from_bar_aligned(
            timestamp=1_700_000_000_000,
            bar={
                "data_ts": 1_700_000_000_000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1200.0,
            },
            symbol=symbol,
        )
        context = {"primary_snapshots": {"ohlcv": snapshot}}
        pred_ctx1 = model.predict_with_context(features, context)
        pred_ctx2 = model.predict_with_context(features, context)
        if isinstance(pred_ctx1, (float, int)):
            _assert_finite(pred_ctx1)
        if isinstance(pred_ctx2, (float, int)):
            _assert_finite(pred_ctx2)
        assert pred_ctx1 == pred_ctx2
        json.dumps(to_jsonable(pred_ctx1))

        covered.add((model_type, type(model).__name__))

    covered_str = ", ".join([f"{mtype}:{cls}" for mtype, cls in sorted(covered)])
    print(f"covered implementations: {covered_str}")
