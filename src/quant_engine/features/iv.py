# src/quant_engine/features/iv.py
import pandas as pd
from quant_engine.contracts.feature import FeatureChannel
from .registry import register_feature


@register_feature("IV30")
class IV30Feature(FeatureChannel):
    """Implied Volatility 30d."""
    def compute(self, df: pd.DataFrame):
        if "iv_30d" not in df:
            return {"iv30": None}
        return {"iv30": float(df["iv_30d"].iloc[-1])}


@register_feature("IV_SKEW")
class IVSkewFeature(FeatureChannel):
    """25d call - 25d put skew."""
    def compute(self, df: pd.DataFrame):
        if "iv_25d_call" not in df or "iv_25d_put" not in df:
            return {"iv_skew": None}
        skew = df["iv_25d_call"].iloc[-1] - df["iv_25d_put"].iloc[-1]
        return {"iv_skew": float(skew)}