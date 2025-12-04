from quant_engine.contracts.risk import RiskProto
from quant_engine.risk.registry import register_risk

@register_risk("STOP_LOSS")
class StopLossRule(RiskProto):
    def __init__(self, key="pnl", max_loss=-0.05):
        self.key = key
        self.max_loss = max_loss

    def adjust(self, size: float, features: dict) -> float:
        pnl = features.get(self.key, 0.0)
        if pnl < self.max_loss:
            return 0.0
        return size