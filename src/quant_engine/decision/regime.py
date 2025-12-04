# decision/regime.py
from quant_engine.contracts.decision import DecisionProto
from .registry import register_decision


@register_decision("REGIME")
class RegimeDecision(DecisionProto):
    def __init__(self, bull: float = 1.0, bear: float = -1.0):
        self.bull = bull
        self.bear = bear

    def decide(self, regime_label: float) -> float:
        if regime_label > 0:
            return self.bull
        elif regime_label < 0:
            return self.bear
        return 0.0