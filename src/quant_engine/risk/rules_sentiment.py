from typing import Dict, Any
from quant_engine.contracts.risk import RiskBase, parse_feature_name
from quant_engine.risk.registry import register_risk


@register_risk("SENTIMENT-SCALE")
class SentimentScaleRule(RiskBase):
    """
    V4 sentiment-based risk scaler.

    Contracts:
    - symbol-aware via RiskBase
    - optional sentiment feature dependency
    - adjust(size, context) -> float
    """

    # design-time capability requirement
    required_feature_types = {"SENTIMENT"}

    def __init__(self, symbol: str, **kwargs):
        strength = kwargs.get("strength", 1.0)
        super().__init__(symbol=symbol, **kwargs)
        self.strength = float(strength)

    def adjust(self, size: float, context: Dict[str, Any]) -> float:
        features = context.get("features", context)

        # Prefer semantic lookup via bound feature index
        try:
            sentiment = float(self.fget(features, ftype="SENTIMENT", purpose="RISK"))
        except Exception:
            # Fallback: locate SENTIMENT feature in validation names
            sentiment_name = None
            for n in getattr(self, "required_features", set()):
                try:
                    t, p, s, r = parse_feature_name(n)
                except Exception:
                    continue
                if t == "SENTIMENT" and p == "RISK" and s == self.symbol:
                    sentiment_name = n
                    break
            if sentiment_name is None:
                sentiment = 0.0
            else:
                sentiment = float(features.get(sentiment_name, 0.0))

        scaled = float(size) * (1.0 + self.strength * float(sentiment))
        return max(-1.0, min(1.0, scaled))
