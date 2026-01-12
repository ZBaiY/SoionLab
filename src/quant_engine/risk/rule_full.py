

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
        """Snap target position to {0,1} without violating upstream constraints."""
        threshold = float(context.get("full_allocation_threshold", 0.5))
        proposed = float(size)

        target = 1.0 if proposed >= threshold else 0.0
        if target < 0.0:
            target = 0.0
        if target > 1.0:
            target = 1.0

        risk_state = context.get("risk_state", {})
        if isinstance(risk_state, dict):
            constrained = risk_state.get("constrained_target_position")
            if constrained is not None:
                try:
                    target = min(target, float(constrained))
                except Exception:
                    pass

        return target
