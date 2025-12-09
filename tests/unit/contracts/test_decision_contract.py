from quant_engine.contracts.decision import DecisionProto

class DummyDecision(DecisionProto):
    def decide(self, context: dict) -> float:
        # Minimal logic: sum available scores if exist, otherwise return 0
        score = 0.0
        if "model_score" in context and isinstance(context["model_score"], (float, int)):
            score += float(context["model_score"])
        if "sentiment_score" in context and isinstance(context["sentiment_score"], (float, int)):
            score += float(context["sentiment_score"])
        if "regime_label" in context and isinstance(context["regime_label"], (float, int)):
            score += float(context["regime_label"])
        return score

def test_decision_contract_output_numeric():
    dec = DummyDecision()
    context = {
        "model_score": 0.3,
        "sentiment_score": -0.1,
        "regime_label": 1,
        "features": {"rsi": 55.0},
        "raw_data": None,
    }
    out = dec.decide(context)
    assert isinstance(out, float)

def test_decision_contract_handles_missing_fields():
    dec = DummyDecision()
    context = {"model_score": 0.5}
    out = dec.decide(context)
    assert isinstance(out, float)

def test_decision_contract_accepts_full_context_structure():
    dec = DummyDecision()
    context = {
        "model_score": 1.0,
        "sentiment_score": 0.2,
        "regime_label": -1,
        "features": {"macd": -0.02},
        "raw_data": "fake_df_or_placeholder",
    }
    out = dec.decide(context)
    assert isinstance(out, float)
