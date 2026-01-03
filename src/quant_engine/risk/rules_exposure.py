from typing import Dict, Any
from quant_engine.contracts.risk import RiskBase
from quant_engine.risk.registry import register_risk


@register_risk("EXPOSURE-LIMIT")
class ExposureLimitRule(RiskBase):
    """
    V4 exposure cap risk rule.

    Contracts:
    - symbol-aware via RiskBase
    - no feature dependency
    - adjust(size, features) -> float
    """

    # no feature dependency
    required_feature_types: set[str] = set()

    def __init__(self, symbol: str, **kwargs):
        limit = kwargs.get("limit", 3.0)
        super().__init__(symbol=symbol, **kwargs)
        self.limit = float(limit)

    def adjust(self, size: float, context: Dict[str, Any]) -> float:
        if abs(size) > self.limit:
            return self.limit * (1 if size > 0 else -1)
        return size