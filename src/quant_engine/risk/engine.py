# risk/engine.py
from quant_engine.contracts.risk import RiskBase

class RiskEngine:

    def __init__(self, rules: list[RiskBase] ,symbol: str = ""):
        self.rules = rules
        self.symbol = symbol

    def apply(self, size: float, context: dict) -> float:
        for rule in self.rules:
            size = rule.adjust(size, context)
        return size