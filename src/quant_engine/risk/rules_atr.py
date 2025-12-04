# risk/rules_atr.py
from quant_engine.contracts.risk import RiskProto
from .registry import register_risk

@register_risk("ATR_SIZE")
class ATRSizeRule(RiskProto):
    def __init__(self, base_size=1.0, atr_key="atr"):
        self.base_size = base_size
        self.atr_key = atr_key

    def adjust(self, size: float, features: dict) -> float:
        atr = features.get(self.atr_key, 1.0)
        return size * self.base_size / (atr + 1e-8)