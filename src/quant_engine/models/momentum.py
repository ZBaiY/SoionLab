# models/momentum.py
from quant_engine.contracts.model import ModelProto
from .registry import register_model


@register_model("RSI_MODEL")
class RSIMomentumModel(ModelProto):
    def __init__(self, overbought=70, oversold=30):
        self.ob = overbought
        self.os = oversold

    def predict(self, features: dict) -> float:
        rsi = features["rsi"]
        if rsi > self.ob:
            return -1.0   # mean reversion short
        elif rsi < self.os:
            return 1.0    # mean reversion long
        return 0.0
    
@register_model("MACD_MODEL")
class MACDMomentumModel(ModelProto):
    def __init__(self):
        pass

    def predict(self, features: dict) -> float:
        macd = features["macd"]
        signal = features.get("macd_signal", 0)
        return 1.0 if macd > signal else -1.0