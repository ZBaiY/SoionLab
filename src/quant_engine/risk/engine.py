# risk/engine.py
from quant_engine.contracts.risk import RiskProto

class RiskEngine:
    def __init__(self, rules: list[RiskProto]):
        self.rules = rules

    def apply(self, size: float, features: dict) -> float:
        for rule in self.rules:
            size = rule.adjust(size, features)
        return size