from quant_engine.contracts.risk import RiskProto
from quant_engine.risk.registry import register_risk

@register_risk("SENTIMENT_SCALE")
class SentimentScaleRule(RiskProto):
    def __init__(self, key="sentiment_score", strength=1.0):
        self.key = key
        self.strength = strength

    def adjust(self, size: float, features: dict) -> float:
        sentiment = features.get(self.key, 0.0)
        return size * (1 + self.strength * sentiment)