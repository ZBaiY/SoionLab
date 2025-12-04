from .registry import build_decision

class DecisionLoader:
    @staticmethod
    def from_config(cfg: dict):
        """
        cfg example:
        {
            "type": "THRESHOLD",
            "params": {"threshold": 0.0}
        }
        """
        name = cfg["type"]
        params = cfg.get("params", {})
        return build_decision(name, **params)