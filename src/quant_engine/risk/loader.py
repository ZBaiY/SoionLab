# risk/loader.py
from .registry import build_risk
from .engine import RiskEngine

class RiskLoader:
    @staticmethod
    def from_config(symbol: str, cfg: dict) -> RiskEngine:
        """
        Create a RiskEngine from configuration.
        """
        rules_cfg = cfg.get("rules", {})
        rules = [
            build_risk(
                name,
                symbol=symbol,
                **rule_cfg.get("params", {}),
            )
            for name, rule_cfg in rules_cfg.items()
        ]
        return RiskEngine(rules, symbol=symbol)