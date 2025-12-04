# features/extractor.py
from quant_engine.contracts.feature import FeatureChannel
from quant_engine.data.historical import HistoricalDataHandler
from quant_engine.data.realtime import RealTimeDataHandler
from .registry import build_feature

"""
extractor = FeatureExtractor(
    historical=hist,
    realtime=rt,
    feature_names=["RSI", "MACD"],
    feature_kwargs={
        "RSI": {"period": 14},
        "MACD": {"fast": 12, "slow": 26},
    }
)
features = extractor.compute()
"""

class FeatureExtractor:
    def __init__(
        self,
        historical: HistoricalDataHandler,
        realtime: RealTimeDataHandler,
        feature_config: dict[str, dict],
    ):
        """
        feature_config example:
        {
            "RSI": {"period": 14},
            "MACD": {"fast": 12, "slow": 26},
            "SPREAD": {},
            "REALIZED_VOL": {"window": 30}
        }
        """
        self.historical = historical
        self.realtime = realtime

        self.channels: list[FeatureChannel] = [
            build_feature(name, **params)
            for name, params in feature_config.items()
        ]

    def compute(self) -> dict:
        df = self.realtime.window_df()
        result = {}
        for ch in self.channels:
            result.update(ch.compute(df))
        return result