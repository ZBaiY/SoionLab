import pytest
from typing import Dict, Any

from quant_engine.contracts.decision import DecisionBase
from quant_engine.decision.registry import register_decision
from quant_engine.decision.loader import DecisionLoader


# ---------------------------------------------------------------------
# Dummy Decisions
# ---------------------------------------------------------------------
@register_decision("THRESHOLD")
class ThresholdDecision(DecisionBase):
    """
    Decision based on a single model score:
        score > threshold → +1
        score < -threshold → -1
        else               → 0
    """

    def __init__(self, threshold: float = 0.0, **kwargs):
        self.threshold = threshold

    def decide(self, context: Dict[str, Any]) -> float:
        score = context.get("model_score")
        if score is None:
            raise KeyError("model_score missing from context")

        if score > self.threshold:
            return 1.0
        if score < -self.threshold:
            return -1.0
        return 0.0


@register_decision("FUSE_MEAN")
class MeanFusionDecision(DecisionBase):
    """
    Fuse multiple model outputs by mean.
    Expects context keys: model_a, model_b, ...
    """

    def decide(self, context: Dict[str, Any]) -> float:
        model_scores = [
            v for k, v in context.items()
            if k != "features" and isinstance(v, (int, float))
        ]

        if not model_scores:
            raise ValueError("No model outputs found for fusion")

        return float(sum(model_scores) / len(model_scores))


@register_decision("REGIME_SWITCH")
class RegimeSwitchDecision(DecisionBase):
    """
    Feature-aware decision:
        • uses regime feature to gate model output
    """

    def __init__(self, bull: float = 1.0, bear: float = -1.0, **kwargs):
        self.bull = bull
        self.bear = bear

    def decide(self, context: Dict[str, Any]) -> float:
        features = context.get("features")
        if features is None:
            raise KeyError("features missing from context")

        regime = features.get("REGIME")
        if regime is None:
            raise KeyError("REGIME feature missing")

        score = context.get("model_score")
        if score is None:
            raise KeyError("model_score missing")

        if regime > 0:
            return self.bull * score
        return self.bear * score


# ---------------------------------------------------------------------
# Loader Tests
# ---------------------------------------------------------------------
def test_decision_loader_threshold():
    cfg = {
        "type": "THRESHOLD",
        "params": {"threshold": 0.5},
    }

    decision = DecisionLoader.from_config(cfg, symbol="BTCUSDT")

    assert isinstance(decision, ThresholdDecision)
    assert decision.threshold == 0.5


# ---------------------------------------------------------------------
# Decision Logic Tests (Single Model)
# ---------------------------------------------------------------------
def test_threshold_decision_long():
    decision = ThresholdDecision(threshold=0.2)

    context = {
        "model_score": 0.8,
        "features": {},
    }

    assert decision.decide(context) == 1.0


def test_threshold_decision_short():
    decision = ThresholdDecision(threshold=0.2)

    context = {
        "model_score": -0.9,
        "features": {},
    }

    assert decision.decide(context) == -1.0


def test_threshold_decision_neutral():
    decision = ThresholdDecision(threshold=0.5)

    context = {
        "model_score": 0.3,
        "features": {},
    }

    assert decision.decide(context) == 0.0


# ---------------------------------------------------------------------
# Multi-Model Fusion Tests
# ---------------------------------------------------------------------
def test_mean_fusion_multiple_models():
    decision = MeanFusionDecision()

    context = {
        "model_rsi": 1.0,
        "model_zscore": -1.0,
        "model_ml": 0.5,
        "features": {},
    }

    out = decision.decide(context)
    assert out == pytest.approx((1.0 - 1.0 + 0.5) / 3)


def test_mean_fusion_no_models_raises():
    decision = MeanFusionDecision()

    context = {
        "features": {},
    }

    with pytest.raises(ValueError):
        decision.decide(context)


# ---------------------------------------------------------------------
# Feature-Aware Decision Tests (Regime)
# ---------------------------------------------------------------------
def test_regime_switch_bull():
    decision = RegimeSwitchDecision(bull=1.0, bear=-1.0)

    context = {
        "model_score": 0.6,
        "features": {"REGIME": 1},
    }

    assert decision.decide(context) == 0.6


def test_regime_switch_bear():
    decision = RegimeSwitchDecision(bull=1.0, bear=-1.0)

    context = {
        "model_score": 0.6,
        "features": {"REGIME": -1},
    }

    assert decision.decide(context) == -0.6


# ---------------------------------------------------------------------
# Edge / Contract Tests
# ---------------------------------------------------------------------
def test_decision_is_pure_function():
    decision = ThresholdDecision(threshold=0.1)

    context = {
        "model_score": 0.5,
        "features": {"X": 1},
    }
    context_copy = dict(context)

    _ = decision.decide(context)

    assert context == context_copy


def test_missing_features_raises():
    decision = RegimeSwitchDecision()

    context = {
        "model_score": 0.5,
    }

    with pytest.raises(KeyError):
        decision.decide(context)
