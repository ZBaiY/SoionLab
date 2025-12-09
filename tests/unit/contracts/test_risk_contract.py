from quant_engine.contracts.risk import RiskProto


class DummyRisk(RiskProto):
    def adjust(self, size: float, features: dict) -> float:
        # Minimal behavior: scale size by ATR if available
        atr = features.get("atr", 1.0)
        if not isinstance(atr, (float, int)) or atr <= 0:
            atr = 1.0
        return size / float(atr)


def test_risk_contract_returns_float():
    risk = DummyRisk()
    out = risk.adjust(1.0, {"atr": 2.0})
    assert isinstance(out, float)


def test_risk_contract_handles_missing_features():
    risk = DummyRisk()
    out = risk.adjust(1.0, {})
    assert isinstance(out, float)


def test_risk_contract_accepts_mixed_feature_dict():
    risk = DummyRisk()
    features = {
        "atr": 3.0,
        "vol": 0.2,
        "note": "ignore",
        "meta": {"extra": 1},
    }
    out = risk.adjust(0.5, features)
    assert isinstance(out, float)
