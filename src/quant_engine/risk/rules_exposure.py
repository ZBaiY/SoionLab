from quant_engine.contracts.risk import RiskProto
from quant_engine.risk.registry import register_risk

@register_risk("EXPOSURE_LIMIT")
class ExposureLimitRule(RiskProto):
    def __init__(self, limit=3.0):
        self.limit = limit

    def adjust(self, size: float, features: dict) -> float:
        if abs(size) > self.limit:
            return self.limit * (1 if size > 0 else -1)
        return size