# src/quant_engine/features/volatility.py
import pandas as pd
from quant_engine.contracts.feature import FeatureChannel
from .registry import register_feature


@register_feature("ATR")
class ATRFeature(FeatureChannel):
    """Average True Range."""
    def __init__(self, period=14):
        self.period = period

    def compute(self, df: pd.DataFrame):
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()

        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(self.period).mean().iloc[-1]
        return {"atr": float(atr)}


@register_feature("REALIZED_VOL")
class RealizedVolFeature(FeatureChannel):
    """Realized volatility via daily returns."""
    def __init__(self, window=30):
        self.window = window

    def compute(self, df: pd.DataFrame):
        returns = df["close"].pct_change().dropna()
        vol = returns.rolling(self.window).std().iloc[-1]
        return {"realized_vol": float(vol)}