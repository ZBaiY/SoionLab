from quant_engine.contracts.decision import DecisionBase
from .registry import build_decision

class DecisionLoader:
    @staticmethod
    def from_config(symbol: str, cfg: dict) -> DecisionBase:
        """
        cfg example:
        {
            "type": "THRESHOLD",
            "params": {"threshold": 0.0}
        }
        """
        name = cfg["type"]
        params = cfg.get("params", {})
        return build_decision(name, symbol=symbol, **params)