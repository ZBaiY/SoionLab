from typing import Dict, Any
from quant_engine.contracts.model import ModelBase
from .registry import register_model

@register_model("PAIR-ZSCORE")
class PairZScoreModel(ModelBase):
    """
    Minimal pair-trading z-score model.
    """

    # design-time feature capability requirement
    required_feature_types = {"SPREAD"}

    def __init__(self, symbol: str, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        secondary = kwargs.get("ref") or kwargs.get("secondary")
        self.secondary = secondary
        if not secondary:
            raise ValueError("PairZScoreModel requires secondary=... in kwargs")
        self.lookback = int(kwargs.get("lookback", 120))
        

    def predict(self, features: Dict[str, Any]) -> float:
        """
        Dummy logic:
        - expects a SPREAD feature keyed by BTCUSDT^ETHUSDT or reverse
        - returns sign(z) placeholder
        """
        if not self.secondary:
            raise ValueError("PairZScoreModel requires secondary symbol (secondary=...)")

        # Preferred: semantic lookup via the bound feature index
        try:
            spread_name = self.fname(ftype="SPREAD", purpose="MODEL", ref=self.secondary)
            spread = features[spread_name]
        except KeyError:
            # Fallback: if index is not bound yet, use the (validated) required feature list
            if not self.required_features:
                return 0.0
            spread_name = next(iter(self.required_features))
            spread = features.get(spread_name)
            if spread is None:
                return 0.0

        # placeholder "z-score"
        if spread > 0:
            return -1.0
        elif spread < 0:
            return 1.0
        return 0.0