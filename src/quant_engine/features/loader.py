# features/loader.py
# features/loader.py

from quant_engine.features.registry import build_feature
from quant_engine.features.extractor import FeatureExtractor
from quant_engine.utils.logger import get_logger, log_debug


class FeatureLoader:
    _logger = get_logger(__name__)

    @staticmethod
    def from_config(cfg: dict, data_handler):
        """
        data_handler expected structure:
        {
            "historical_ohlcv": HistoricalDataHandler,
            "realtime_ohlcv":   RealTimeDataHandler,
            "option_chain":     OptionChainDataHandler (optional),
            "sentiment":        SentimentLoader (optional)
        }
        
        cfg["features"] MUST look like:
        [
            { "type": "RSI", "symbol": "BTCUSDT", "params": {...}},
            { "type": "RSI", "symbol": "ETHUSDT", "params": {...}},
            { "type": "MACD", "symbol": "ETHUSDT", "params": {...}},
            ...
        ]
        
        ----> again, one single symbol is invested, others are for features/models
        ----> this engine is for single symbol only.
        """

        log_debug(FeatureLoader._logger, "FeatureLoader received config", config=cfg)

        feature_config = []
        for f in cfg["features"]:
            feature_config.append({
                "type": f["type"],
                "symbol": f.get("symbol"),   # may be None
                "params": f.get("params", {})
            })


        return FeatureExtractor(
            historical_ohlcv=data_handler.historical_ohlcv,
            realtime_ohlcv=data_handler.realtime_ohlcv,
            option_chain_handler=getattr(data_handler, "option_chain", None),
            sentiment_loader=getattr(data_handler, "sentiment", None),
            feature_config=feature_config
        )