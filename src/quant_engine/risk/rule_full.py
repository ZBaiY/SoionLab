

from __future__ import annotations

from typing import Any, Dict

from quant_engine.contracts.risk import RiskBase
from quant_engine.utils.logger import get_logger, log_debug
from .registry import register_risk

_LOG = get_logger(__name__)


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
                except (TypeError, ValueError) as exc:
                    # Constraint parse failures are non-fatal, but must stay observable.
                    log_debug(
                        _LOG,
                        "risk.full_allocation.constraint.suppressed",
                        err_type=type(exc).__name__,
                        err=str(exc),
                    )

        return target
