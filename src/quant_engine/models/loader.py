# models/loader.py
import json
from .registry import build_model

class ModelLoader:
    @staticmethod
    def from_config(model_cfg: dict):
        """
        model_cfg example:
        {
            "type": "OU_MODEL",
            "params": {"theta": 1, "mu": 50000, "sigma": 0.05}
        }
        """
        name = model_cfg["type"]
        params = model_cfg.get("params", {})
        return build_model(name, **params)