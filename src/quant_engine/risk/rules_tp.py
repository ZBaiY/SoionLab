from quant_engine.contracts.risk import RiskProto
from quant_engine.risk.registry import register_risk

@register_risk("TAKE_PROFIT")
class TakeProfitRule(RiskProto):
    def __init__(self, max_gain=0.1):
        self.max_gain = max_gain

    def size(self, intent: float, volatility: float = 1.0) -> float:
        if intent > self.max_gain:
            return 0.0
        return intent