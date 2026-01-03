

from __future__ import annotations

from typing import Any, Dict

from quant_engine.contracts.risk import RiskBase
from .registry import register_risk


@register_risk("FULL-ALLOCATION")
class FullAllocation(RiskBase):
    """V4 full-allocation risk rule.

    Semantics:
    - No feature dependencies.
    - `adjust(size, context)` returns the input size unchanged.

    Intended use:
    - Strategies that want to allocate 100% of the decision-supplied size
      (subject to downstream execution/portfolio constraints).
    """

    # No feature requirements
    required_feature_types: set[str] = set()

    def __init__(self, symbol: str, **kwargs: Any):
        super().__init__(symbol=symbol, **kwargs)

    def adjust(self, size: float, context: Dict[str, Any]) -> float:
        # Full allocation: do not scale down the proposed size.
        return float(size)