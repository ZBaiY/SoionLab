from typing import Any, Dict
from quant_engine.contracts.risk import RiskBase, parse_feature_name
from .registry import register_risk

@register_risk("ATR-SIZER")
class ATRSizer(RiskBase):
    """
    V4 ATR-based position sizer.

    Contracts:
    - symbol-aware via RiskBase
    - declares ATR dependency for feature resolver
    - adjust(size, features) -> float
    """

    # design-time capability requirement
    required_feature_types = {"ATR"}

    def __init__(self, symbol: str, **kwargs):
        risk_fraction = kwargs.get("risk_fraction", 0.02)
        super().__init__(symbol=symbol, **kwargs)
        self.risk_fraction = float(risk_fraction)

    def adjust(self, size: float, context: Dict[str, Any]) -> float:
        """Scale position size inversely with ATR."""
        features = context.get("features", context)

        # Prefer semantic lookup via bound feature index
        try:
            atr = float(self.fget(features, ftype="ATR", purpose="RISK"))
        except Exception:
            # Fallback: locate ATR feature in validation names
            atr_name = None
            for n in getattr(self, "required_features", set()):
                try:
                    t, p, s, r = parse_feature_name(n)
                except Exception:
                    continue
                if t == "ATR" and p == "RISK" and s == self.symbol:
                    atr_name = n
                    break
            if atr_name is None:
                atr = 1.0
            else:
                atr = float(features.get(atr_name, 1.0))

        scaled = float(size) * self.risk_fraction / max(float(atr), 1e-8)
        return max(-1.0, min(1.0, scaled))
