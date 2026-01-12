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
        rules = []
        for name, rule_cfg in rules_cfg.items():
            if isinstance(rule_cfg, dict) and "space" in rule_cfg:
                space = str(rule_cfg.get("space")).lower()
                if space != "target":
                    raise ValueError(
                        f"Risk rule '{name}' declares space='{space}', but Risk only accepts target rules."
                    )
            rule = build_risk(
                name,
                symbol=symbol,
                **rule_cfg.get("params", {}),
            )
            rules.append(rule)
        return RiskEngine(rules, symbol=symbol, risk_config=cfg)
