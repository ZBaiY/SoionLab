# risk/engine.py
from collections.abc import Iterable

from quant_engine.contracts.risk import RiskBase

class RiskEngine:

    def __init__(self, rules: list[RiskBase], symbol: str = ""):
        self.rules = rules
        self.symbol = symbol

    def adjust(self, size: float, context: dict) -> float:
        for rule in self.rules:
            size = rule.adjust(size, context)
        return size

    def set_required_features(self, feature_names: Iterable[str]) -> None:
        for rule in self.rules:
            if hasattr(rule, "set_required_features"):
                rule.set_required_features(feature_names)

    def validate_features(self, available_features: set[str]) -> None:
        for rule in self.rules:
            if hasattr(rule, "validate_features"):
                rule.validate_features(available_features)

    def validate_feature_types(self, available_feature_types: set[str]) -> None:
        for rule in self.rules:
            if hasattr(rule, "validate_feature_types"):
                rule.validate_feature_types(available_feature_types)
