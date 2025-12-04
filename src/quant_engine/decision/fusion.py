# decision/fusion.py
from quant_engine.contracts.decision import DecisionProto
from .registry import register_decision


@register_decision("FUSION")
class FusionDecision(DecisionProto):
    """
    Combine multiple model scores:
        score_final = w1 * model1 + w2 * model2 + ...
    """
    def __init__(self, weights: dict[str, float]):
        self.weights = weights

    def decide(self, context: dict) -> float:
        # context: {"model_score": ..., "sent_score": ..., ...}
        total = 0.0
        for key, w in self.weights.items():
            total += w * context.get(key, 0.0)
        return total