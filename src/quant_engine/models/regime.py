# models/regime.py
from typing import Dict, Any
from quant_engine.contracts.model import ModelBase
from quant_engine.models.registry import register_model


@register_model("VOL-REGIME")
class VolRegimeModel(ModelBase):
    """
    V4 volatility regime classifier.

    Contracts:
    - symbol-aware via ModelBase
    - declares VOLATILITY dependency
    - predict(features) -> float
    """

    # design-time capability requirement
    required_feature_types = {"VOLATILITY"}

    def __init__(self, symbol: str, **kwargs):
        # threshold is a model parameter; keep it in kwargs for loader-style construction
        threshold = kwargs.get("threshold", 0.02)
        super().__init__(symbol=symbol, **kwargs)
        self.threshold = float(threshold)

    def predict(self, features: Dict[str, Any]) -> float:
        # Prefer semantic lookup via the bound feature index
        try:
            vol = float(self.fget(features, ftype="VOLATILITY", purpose="MODEL"))
        except Exception:
            # Fallback: use (validated) required feature names if index is not bound yet
            if not getattr(self, "required_features", None):
                vol = 0.0
            else:
                name = next(iter(self.required_features))
                vol = float(features.get(name, 0.0))
        return 1.0 if vol > self.threshold else -1.0