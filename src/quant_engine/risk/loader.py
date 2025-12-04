# risk/loader.py
from .registry import build_risk
from .engine import RiskEngine

class RiskLoader:
    @staticmethod
    def from_config(cfg: dict):
        rules_cfg = cfg.get("rules", {})
        rules = [build_risk(name, **params) for name, params in rules_cfg.items()]
        return RiskEngine(rules)