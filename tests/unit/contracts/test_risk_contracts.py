import math
import pytest
from typing import Dict, Any

from quant_engine.contracts.risk import RiskBase
from quant_engine.risk.engine import RiskEngine
from quant_engine.risk.loader import RiskLoader
from quant_engine.risk.registry import register_risk


# ---------------------------------------------------------------------
# Dummy Risk Rules (contract / pipeline testing)
# ---------------------------------------------------------------------
@register_risk("CLIP")
class ClipRisk(RiskBase):
    """Clip target position to [-max_abs, +max_abs]."""

    def __init__(self, symbol: str, max_abs: float = 1.0, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        self.max_abs = float(max_abs)

    def adjust(self, size: float, features: Dict[str, Any]) -> float:
        # Do not mutate features
        _ = features.get("noop", None)

        if math.isnan(size):
            raise ValueError("size is NaN")
        if math.isinf(size):
            return math.copysign(self.max_abs, size)

        return max(-self.max_abs, min(self.max_abs, float(size)))


@register_risk("ATR_SCALE")
class AtrScaleRisk(RiskBase):
    """
    Scale size by ATR: size / (1 + k*ATR).
    Requires standardized key: ATR_<SYMBOL>
    """

    required_features = ["ATR"]

    def __init__(self, symbol: str, k: float = 1.0, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        self.k = float(k)

    def adjust(self, size: float, features: Dict[str, Any]) -> float:
        # symbol-aware feature filtering (contract)
        filtered = self.filter_symbol(features)

        key = f"ATR_{self.symbol}"
        if key not in filtered:
            raise KeyError("ATR feature missing")

        atr = float(filtered[key])
        if atr < 0:
            raise ValueError("ATR must be non-negative")

        denom = 1.0 + self.k * atr
        if denom == 0.0:
            raise ZeroDivisionError("invalid ATR scaling denominator")

        return float(size) / denom


@register_risk("SENTIMENT_GATE")
class SentimentGateRisk(RiskBase):
    """
    Gate size to 0 when sentiment is below threshold.
    Requires standardized key: SENTIMENT_<SYMBOL>
    """

    required_features = ["SENTIMENT"]

    def __init__(self, symbol: str, threshold: float = -0.5, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        self.threshold = float(threshold)

    def adjust(self, size: float, features: Dict[str, Any]) -> float:
        filtered = self.filter_symbol(features)

        key = f"SENTIMENT_{self.symbol}"
        if key not in filtered:
            raise KeyError("SENTIMENT feature missing")

        s = float(filtered[key])
        if s < self.threshold:
            return 0.0
        return float(size)


# ---------------------------------------------------------------------
# Base / Contract tests
# ---------------------------------------------------------------------
def test_risk_base_required_features_are_suffixed_per_symbol():
    r = AtrScaleRisk(symbol="BTCUSDT", k=1.0)
    # RiskBase.__init__ materializes required_features with symbol suffix
    assert r.required_features == ["ATR_BTCUSDT"]


def test_risk_filter_symbol_keeps_only_primary_symbol_keys():
    r = ClipRisk(symbol="BTCUSDT", max_abs=1.0)

    features = {
        "ATR_BTCUSDT": 1.0,
        "ATR_ETHUSDT": 999.0,
        "SENTIMENT_BTCUSDT": 0.2,
        "ZSCORE_ETHUSDT^BTCUSDT": 5.0,  # should not match suffix rule
    }
    filtered = r.filter_symbol(features)

    assert filtered == {
        "ATR_BTCUSDT": 1.0,
        "SENTIMENT_BTCUSDT": 0.2,
    }


# ---------------------------------------------------------------------
# Engine tests (rule chaining)
# ---------------------------------------------------------------------
def test_risk_engine_applies_rules_in_order_non_commutative():
    """
    Order matters:
        - If we gate first, size becomes 0 and clip has no effect.
        - If we clip first, still can be gated to 0.
    We lock the engine semantics: applies rules in list order.
    """
    symbol = "BTCUSDT"
    features = {
        "SENTIMENT_BTCUSDT": -0.9,  # triggers gate -> 0
        "ATR_BTCUSDT": 0.0,
    }

    # clip then gate
    eng1 = RiskEngine([ClipRisk(symbol=symbol, max_abs=0.3), SentimentGateRisk(symbol=symbol, threshold=-0.5)])
    out1 = eng1.apply(10.0, features)
    assert out1 == 0.0

    # gate then clip
    eng2 = RiskEngine([SentimentGateRisk(symbol=symbol, threshold=-0.5), ClipRisk(symbol=symbol, max_abs=0.3)])
    out2 = eng2.apply(10.0, features)
    assert out2 == 0.0

    # now remove gate to expose clip effect
    eng3 = RiskEngine([ClipRisk(symbol=symbol, max_abs=0.3), AtrScaleRisk(symbol=symbol, k=0.0)])
    out3 = eng3.apply(10.0, features)
    assert out3 == 0.3


def test_risk_engine_no_rules_is_identity():
    eng = RiskEngine([])
    features = {"ATR_BTCUSDT": 1.0}
    assert eng.apply(0.7, features) == 0.7


def test_risk_engine_preserves_feature_dict_purity():
    eng = RiskEngine([AtrScaleRisk(symbol="BTCUSDT", k=2.0)])

    features = {"ATR_BTCUSDT": 0.5, "X": 1}
    features_copy = dict(features)

    _ = eng.apply(1.0, features)

    assert features == features_copy


# ---------------------------------------------------------------------
# Loader tests (config -> engine -> rules)
# ---------------------------------------------------------------------
def test_risk_loader_builds_engine_with_multiple_rules():
    cfg = {
        "rules": {
            "CLIP": {"max_abs": 0.2},
            "ATR_SCALE": {"k": 1.5},
            "SENTIMENT_GATE": {"threshold": -0.2},
        }
    }

    eng = RiskLoader.from_config(cfg, symbol="BTCUSDT")
    assert isinstance(eng, RiskEngine)
    assert len(eng.rules) == 3
    assert [r.__class__.__name__ for r in eng.rules] == ["ClipRisk", "AtrScaleRisk", "SentimentGateRisk"]


# ---------------------------------------------------------------------
# Rule-level edge cases
# ---------------------------------------------------------------------
def test_clip_risk_handles_inf_and_rejects_nan():
    r = ClipRisk(symbol="BTCUSDT", max_abs=0.5)
    assert r.adjust(float("inf"), {}) == 0.5
    assert r.adjust(float("-inf"), {}) == -0.5
    with pytest.raises(ValueError):
        _ = r.adjust(float("nan"), {})


def test_atr_scale_missing_feature_raises():
    r = AtrScaleRisk(symbol="BTCUSDT", k=1.0)
    with pytest.raises(KeyError):
        _ = r.adjust(1.0, {"ATR_ETHUSDT": 1.0})


def test_atr_scale_negative_atr_raises():
    r = AtrScaleRisk(symbol="BTCUSDT", k=1.0)
    with pytest.raises(ValueError):
        _ = r.adjust(1.0, {"ATR_BTCUSDT": -0.1})


def test_sentiment_gate_missing_feature_raises():
    r = SentimentGateRisk(symbol="BTCUSDT", threshold=-0.5)
    with pytest.raises(KeyError):
        _ = r.adjust(1.0, {"SENTIMENT_ETHUSDT": -1.0})


def test_sentiment_gate_triggers_and_passes_through():
    r = SentimentGateRisk(symbol="BTCUSDT", threshold=-0.5)

    assert r.adjust(1.0, {"SENTIMENT_BTCUSDT": -0.9}) == 0.0
    assert r.adjust(-0.7, {"SENTIMENT_BTCUSDT": 0.1}) == -0.7
