# decision/threshold.py
from quant_engine.contracts.decision import DecisionProto
from .registry import register_decision


@register_decision("THRESHOLD")
class ThresholdDecision(DecisionProto):
    def __init__(self, threshold: float = 0.0):
        self.threshold = threshold

    def decide(self, score: float) -> float:
        return 1.0 if score > self.threshold else -1.0