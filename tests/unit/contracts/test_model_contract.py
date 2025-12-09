from quant_engine.contracts.model import ModelProto


class DummyModel(ModelProto):
    def predict(self, features: dict) -> float:
        # Minimal behavior: sum numeric features
        total = 0.0
        for v in features.values():
            if isinstance(v, (float, int)):
                total += float(v)
        return total


def test_model_contract_returns_float():
    model = DummyModel()
    features = {"rsi": 55.0, "macd": -0.02}

    out = model.predict(features)
    assert isinstance(out, float)


def test_model_contract_accepts_empty_features():
    model = DummyModel()
    out = model.predict({})
    assert isinstance(out, float)


def test_model_contract_handles_mixed_feature_types():
    model = DummyModel()
    features = {
        "rsi": 50.0,
        "sentiment": 0.3,
        "note": "ignore_this",
        "meta": {"extra": "ignored"},
    }

    out = model.predict(features)
    assert isinstance(out, float)
