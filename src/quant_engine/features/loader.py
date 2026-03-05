# features/loader.py

from quant_engine.features.extractor import FeatureExtractor
from quant_engine.health.manager import HealthManager
from quant_engine.utils.logger import get_logger, log_debug


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
        trades_handlers,
        option_trades_handlers,
        health: HealthManager | None = None,
    ):
        """Build the unified feature extractor from handler maps and feature config list."""

        log_debug(FeatureLoader._logger, "FeatureLoader received config", config=feature_config_list)

        return FeatureExtractor(
            ohlcv_handlers=ohlcv_handlers,
            orderbook_handlers=orderbook_handlers,
            option_chain_handlers=option_chain_handlers,
            iv_surface_handlers=iv_surface_handlers,
            sentiment_handlers=sentiment_handlers,
            trades_handlers=trades_handlers,
            option_trades_handlers=option_trades_handlers,
            feature_config=feature_config_list,
            health=health,
        )
