# src/quant_engine/features/microstructure.py
import pandas as pd
from quant_engine.contracts.feature import FeatureChannel
from .registry import register_feature


@register_feature("SPREAD")
class SpreadFeature(FeatureChannel):
    """Best bid/ask spread."""
    def compute(self, df: pd.DataFrame):
        spread = df["ask"] - df["bid"]
        return {"spread": float(spread.iloc[-1])}


@register_feature("IMBALANCE")
class OrderImbalanceFeature(FeatureChannel):
    """Orderbook imbalance = (bid_size - ask_size) / (sum)."""
    def compute(self, df: pd.DataFrame):
        num = df["bid_size"] - df["ask_size"]
        den = df["bid_size"] + df["ask_size"] + 1e-9
        imbalance = num / den
        return {"order_imbalance": float(imbalance.iloc[-1])}