# features/loader.py

from quant_engine.features.registry import build_feature
from quant_engine.features.extractor import FeatureExtractor


class FeatureLoader:
    @staticmethod
    def from_config(cfg: dict, data_handler):
        """
        cfg example:
        {
            "features": [
                {"type": "RSI", "params": {"period": 14}},
                {"type": "MACD", "params": {"fast": 12, "slow": 26}},
                {"type": "VOLATILITY", "params": {"window": 30}}
            ]
        }
        """

        return FeatureExtractor(
            historical=data_handler.historical,
            realtime=data_handler.realtime,
            feature_config={cfg["type"]: cfg.get("params", {}) for cfg in cfg["features"]}
        )