import pytest
from typing import Dict, Any

from quant_engine.models.loader import ModelLoader
from quant_engine.contracts.model import ModelBase
from quant_engine.models.registry import register_model


# ---------------------------------------------------------------------
# Dummy Pair Z-Score Model for Contract / Pipeline Testing
# ---------------------------------------------------------------------
@register_model("PAIR_ZSCORE")
class DummyPairZScoreModel(ModelBase):
    """
    Minimal stat-arb style model:
        score = zscore if |z| > threshold else 0
    """
    required_features = ["ZSCORE"]
    features_secondary = ["ZSCORE"]

    def __init__(self, symbol: str, lookback: int = 120, threshold: float = 1.0, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        self.lookback = lookback
        self.threshold = threshold

    def predict(self, features: Dict[str, Any]) -> float:
        filtered = self.filter_pair(features)

        # Expected standardized key
        key_1 = f"ZSCORE_{self.secondary}^{self.symbol}"
        key_2 = f"ZSCORE_{self.symbol}^{self.secondary}"

        if key_1 in filtered:
            z = filtered[key_1]
        elif key_2 in filtered:
            z = -filtered[key_2]  # enforce sign convention
        else:
            raise KeyError("ZSCORE pair feature missing")

        if z > self.threshold:
            return 1.0
        if z < -self.threshold:
            return -1.0
        return 0.0


# ---------------------------------------------------------------------
# Loader / Config Tests
# ---------------------------------------------------------------------
def test_model_loader_pair_model_from_config():
    cfg = {
        "type": "PAIR_ZSCORE",
        "params": {
            "secondary": "ETHUSDT",
            "lookback": 120,
            "threshold": 2.0,
        },
    }

    model = ModelLoader.from_config(cfg, symbol="BTCUSDT")

    assert isinstance(model, DummyPairZScoreModel)
    assert model.symbol == "BTCUSDT"
    assert model.secondary == "ETHUSDT"
    assert model.lookback == 120
    assert model.threshold == 2.0


# ---------------------------------------------------------------------
# Prediction Pipeline Tests
# ---------------------------------------------------------------------
def test_pair_model_predict_positive_signal():
    model = DummyPairZScoreModel(symbol="BTCUSDT", secondary="ETHUSDT", threshold=1.0)

    features = {
        "ZSCORE_ETHUSDT^BTCUSDT": 1.8,
        "RSI_BTCUSDT": 55.0,  # noise, should be ignored
    }

    score = model.predict(features)
    assert score == 1.0


def test_pair_model_predict_negative_signal_with_reverse_key():
    model = DummyPairZScoreModel(symbol="BTCUSDT", secondary="ETHUSDT", threshold=1.0)

    features = {
        "ZSCORE_BTCUSDT^ETHUSDT": 2.2,  # reversed order
    }

    score = model.predict(features)
    assert score == -1.0


def test_pair_model_predict_neutral_zone():
    model = DummyPairZScoreModel(symbol="BTCUSDT", secondary="ETHUSDT", threshold=2.0)

    features = {
        "ZSCORE_ETHUSDT^BTCUSDT": 1.5,
    }

    score = model.predict(features)
    assert score == 0.0


# ---------------------------------------------------------------------
# Edge / Failure Mode Tests
# ---------------------------------------------------------------------
def test_pair_model_missing_required_feature_raises():
    model = DummyPairZScoreModel(symbol="BTCUSDT", secondary="ETHUSDT")

    features = {
        "RSI_BTCUSDT": 30.0,
    }

    with pytest.raises(KeyError):
        model.predict(features)


def test_filter_pair_drops_unrelated_symbols():
    model = DummyPairZScoreModel(symbol="BTCUSDT", secondary="ETHUSDT")

    features = {
        "ZSCORE_ETHUSDT^BTCUSDT": 1.2,
        "ZSCORE_SOLUSDT^BTCUSDT": 9.9,
        "RSI_SOLUSDT": 10.0,
    }

    filtered = model.filter_pair(features)

    assert "ZSCORE_ETHUSDT^BTCUSDT" in filtered
    assert "ZSCORE_SOLUSDT^BTCUSDT" not in filtered
    assert "RSI_SOLUSDT" not in filtered


def test_model_predict_is_pure_function():
    """
    predict() must not mutate the input features dict.
    """
    model = DummyPairZScoreModel(symbol="BTCUSDT", secondary="ETHUSDT")

    features = {
        "ZSCORE_ETHUSDT^BTCUSDT": 3.0,
    }
    features_copy = dict(features)

    _ = model.predict(features)

    assert features == features_copy


# ---------------------------------------------------------------------
# Dummy RSI Range Model (Single-Symbol, Non-Paired)
# ---------------------------------------------------------------------
@register_model("RSI_RANGE")
class DummyRSIRangeModel(ModelBase):
    """
    Simple RSI range-based predictor:
        • RSI < low  → +1
        • RSI > high → -1
        • else       → 0
    """
    required_features = ["RSI"]

    def __init__(self, symbol: str, low: float = 30.0, high: float = 70.0, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        self.low = low
        self.high = high

    def predict(self, features: Dict[str, Any]) -> float:
        filtered = self.filter_symbol(features)

        key = f"RSI_{self.symbol}"
        if key not in filtered:
            raise KeyError("RSI feature missing")

        rsi = filtered[key]

        if rsi < self.low:
            return 1.0
        if rsi > self.high:
            return -1.0
        return 0.0


# ---------------------------------------------------------------------
# Loader / Config Tests (Single Symbol)
# ---------------------------------------------------------------------
def test_model_loader_rsi_range_from_config():
    cfg = {
        "type": "RSI_RANGE",
        "params": {
            "low": 25.0,
            "high": 75.0,
        },
    }

    model = ModelLoader.from_config(cfg, symbol="BTCUSDT")

    assert isinstance(model, DummyRSIRangeModel)
    assert model.symbol == "BTCUSDT"
    assert model.secondary is None
    assert model.low == 25.0
    assert model.high == 75.0


# ---------------------------------------------------------------------
# Prediction Pipeline Tests (Single Symbol)
# ---------------------------------------------------------------------
def test_rsi_model_long_signal():
    model = DummyRSIRangeModel(symbol="BTCUSDT", low=30.0, high=70.0)

    features = {
        "RSI_BTCUSDT": 20.0,
        "RSI_ETHUSDT": 90.0,  # noise
    }

    score = model.predict(features)
    assert score == 1.0


def test_rsi_model_short_signal():
    model = DummyRSIRangeModel(symbol="BTCUSDT", low=30.0, high=70.0)

    features = {
        "RSI_BTCUSDT": 85.0,
    }

    score = model.predict(features)
    assert score == -1.0


def test_rsi_model_neutral_zone():
    model = DummyRSIRangeModel(symbol="BTCUSDT", low=30.0, high=70.0)

    features = {
        "RSI_BTCUSDT": 50.0,
    }

    score = model.predict(features)
    assert score == 0.0


# ---------------------------------------------------------------------
# Edge / Failure Mode Tests (Single Symbol)
# ---------------------------------------------------------------------
def test_rsi_model_missing_feature_raises():
    model = DummyRSIRangeModel(symbol="BTCUSDT")

    features = {
        "RSI_ETHUSDT": 10.0,
    }

    with pytest.raises(KeyError):
        model.predict(features)


def test_filter_symbol_excludes_other_symbols():
    model = DummyRSIRangeModel(symbol="BTCUSDT")

    features = {
        "RSI_BTCUSDT": 40.0,
        "RSI_ETHUSDT": 10.0,
        "ZSCORE_ETHUSDT^BTCUSDT": 5.0,
    }

    filtered = model.filter_symbol(features)

    assert filtered == {"RSI_BTCUSDT": 40.0}


def test_rsi_model_predict_is_pure_function():
    model = DummyRSIRangeModel(symbol="BTCUSDT")

    features = {
        "RSI_BTCUSDT": 22.0,
    }
    features_copy = dict(features)

    _ = model.predict(features)

    assert features == features_copy
