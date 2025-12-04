# features/ta.py
from quant_engine.contracts.feature import FeatureChannel
from .registry import register_feature
import pandas as pd

@register_feature("RSI")
class RSIFeature(FeatureChannel):
    def __init__(self, period: int = 14):
        self.period = period

    def compute(self, df: pd.DataFrame):
        delta = df["close"].diff()
        up = delta.clip(lower=0).rolling(self.period).mean()
        down = (-delta.clip(upper=0)).rolling(self.period).mean()
        rsi = up.iloc[-1] / (up.iloc[-1] + down.iloc[-1] + 1e-12)
        return {"rsi": float(rsi)}


@register_feature("MACD")
class MACDFeature(FeatureChannel):
    def __init__(self, fast=12, slow=26, signal=9):
        self.fast = fast
        self.slow = slow
        self.signal = signal

    def compute(self, df):
        fast_ema = df["close"].ewm(span=self.fast).mean()
        slow_ema = df["close"].ewm(span=self.slow).mean()
        macd = fast_ema - slow_ema
        return {"macd": float(macd.iloc[-1])}