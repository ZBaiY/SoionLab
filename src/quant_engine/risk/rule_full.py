

from __future__ import annotations

from typing import Any, Dict

from quant_engine.contracts.risk import RiskBase
from .registry import register_risk


@register_risk("FULL-ALLOCATION")
class FullAllocation(RiskBase):
    """Full-allocation rule (0/1 target)."""
    PRIORITY = 10

    # No feature requirements
    required_feature_types: set[str] = set()

    def __init__(self, symbol: str, **kwargs: Any):
        super().__init__(symbol=symbol, **kwargs)

    def adjust(self, size: float, context: Dict[str, Any]) -> float:
        """Snap target position to {0,1} without violating upstream constraints."""
        threshold = float(context.get("full_allocation_threshold", 0.5))
        proposed = float(size)

        target = 1.0 if proposed >= threshold else 0.0

        risk_state = context.get("risk_state", {})
        if isinstance(risk_state, dict):
            constrained = risk_state.get("constrained_target_position")
            if constrained is not None:
                try:
                    target = min(target, float(constrained))
                except Exception:
                    pass

        return target
