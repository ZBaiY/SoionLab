# features/loader.py

from quant_engine.features.registry import build_feature
from quant_engine.features.extractor import FeatureExtractor
from quant_engine.utils.logger import get_logger, log_debug
#### 有些feature需要对手标的，注意param里应该包含ref字段。


class FeatureLoader:
    _logger = get_logger(__name__)

    @staticmethod
    def from_config(
        feature_config_list: list,
        ohlcv_handlers,
        orderbook_handlers,
        option_chain_handlers,
        iv_surface_handlers,
        sentiment_handlers,
    ):
        """
        Build FeatureExtractor from Version 3 feature config.

        feature_config_list:
            [
                {"type": "...", "symbol": "...", "params": {...}},
                #### 有些feature需要对手标的，注意param里应该包含ref字段。
                ...
            ]

        Handlers are passed as multi-symbol mappings (symbol → handler):
            ohlcv_handlers:        dict[str, RealTimeDataHandler]
            orderbook_handlers:    dict[str, RealTimeOrderbookHandler]
            option_chain_handlers: dict[str, OptionChainDataHandler]
            iv_surface_handlers:   dict[str, IVSurfaceDataHandler]
            sentiment_handlers:    dict[str, SentimentLoader]
        """

        log_debug(FeatureLoader._logger, "FeatureLoader received config", config=feature_config_list)

        return FeatureExtractor(
            ohlcv_handlers=ohlcv_handlers,
            orderbook_handlers=orderbook_handlers,
            option_chain_handlers=option_chain_handlers,
            iv_surface_handlers=iv_surface_handlers,
            sentiment_handlers=sentiment_handlers,
            feature_config=feature_config_list,
        )