from __future__ import annotations
from typing import Optional, Dict, Any, List

from quant_engine.contracts.feature import FeatureChannel
from quant_engine.data.ohlcv.historical import HistoricalDataHandler
from quant_engine.data.ohlcv.realtime import RealTimeDataHandler
from quant_engine.data.derivatives.chain_handler import OptionChainDataHandler
from quant_engine.sentiment.loader import SentimentLoader
from quant_engine.data.orderbook.realtime import RealTimeOrderbookHandler
from quant_engine.data.orderbook.historical import HistoricalOrderbookHandler
from .registry import build_feature

from quant_engine.utils.logger import get_logger, log_debug


class FeatureExtractor:
    """
    Unified feature extractor — multi-source and multi-symbol ready.

    Context passed to each FeatureChannel:
    {
        "realtime_ohlcv": RealTimeDataHandler,
        "orderbook_realtime": RealTimeOrderbookHandler | None,
        "realtime": RealTimeDataHandler,
        "option_chain": OptionChainDataHandler | None,
        "sentiment": SentimentLoader | None,
    }

    Each FeatureChannel: compute(context) -> dict[str, float]
    """

    _logger = get_logger(__name__)

    def __init__(
        self,
        realtime_ohlcv: RealTimeDataHandler,
        realtime_orderbook: Optional[RealTimeOrderbookHandler] = None,
        option_chain_handler: Optional[OptionChainDataHandler] = None,
        sentiment_loader: Optional[SentimentLoader] = None,
        feature_config: List[Dict[str, Any]] | None = None,
    ):
        log_debug(self._logger, "Initializing FeatureExtractor")

        self.realtime_ohlcv = realtime_ohlcv
        self.realtime_orderbook = realtime_orderbook
        self.option_chain_handler = option_chain_handler
        self.sentiment_loader = sentiment_loader

        feature_config = feature_config or []

        self.channels = [
            build_feature(
                item["type"],
                symbol=item.get("symbol"),
                **item.get("params", {})
            )
            for item in feature_config
        ]

        log_debug(
            self._logger,
            "FeatureExtractor channels loaded",
            channels=[type(c).__name__ for c in self.channels]
        )

        self._initialized = False
        self._last_ts = None
        self._last_output = {}

    # ----------------------------------------------------------------------
    # Full-window initialization
    # ----------------------------------------------------------------------
    def initialize(self) -> Dict[str, Any]:
        """
        Perform full-window initialization (historical warmup).
        Called on backtest startup or live cold-start.
        """
        # Determine required full-window size across all features
        max_window = max((ch.required_window() for ch in self.channels), default=1)
        ohlcv_window = self.realtime_ohlcv.window_df(max_window)

        context = {
            "realtime_ohlcv": ohlcv_window,
            "realtime": self.realtime_ohlcv,
            "orderbook_realtime": self.realtime_orderbook,
            "option_chain": self.option_chain_handler,
            "sentiment": self.sentiment_loader,
        }

        # initialize all channels
        for ch in self.channels:
            ch.initialize(context)

        # store initial output
        self._last_output = self.compute_output()
        self._initialized = True
        self._last_ts = self.realtime_ohlcv.last_timestamp()

        return self._last_output

    # ----------------------------------------------------------------------
    # Incremental update
    # ----------------------------------------------------------------------
    def update(self) -> Dict[str, Any]:
        """
        Incremental update for new bar arrival.
        Uses only the latest bar and latest option/sentiment data.
        """
        # If not initialized → perform warmup
        if not self._initialized:
            return self.initialize()

        ts = self.realtime_ohlcv.last_timestamp()
        if ts == self._last_ts:
            return self._last_output   # no new bar

        new_bar = self.realtime_ohlcv.latest_bar()

        context = {
            "ohlcv": new_bar,   # IMPORTANT — only the newest bar
            "realtime": self.realtime_ohlcv,
            "orderbook_realtime": self.realtime_orderbook,
            "option_chain": self.option_chain_handler,
            "sentiment": self.sentiment_loader,
        }

        # incremental update
        for ch in self.channels:
            ch.update(context)

        self._last_output = self.compute_output()
        self._last_ts = ts
        return self._last_output

    # ----------------------------------------------------------------------
    # Output aggregator
    # ----------------------------------------------------------------------
    def compute_output(self) -> Dict[str, Any]:
        """
        Collect feature outputs from all channels.
        """
        result: Dict[str, Any] = {}
        for ch in self.channels:
            result.update(ch.output())
        return result

    # ----------------------------------------------------------------------
    # Thin wrapper (keeps old API)
    # ----------------------------------------------------------------------
    def compute(self) -> Dict[str, Any]:
        """
        Kept for backward compatibility.
        In v4 this performs an incremental update.
        """
        out = self.update()
        log_debug(self._logger, "FeatureExtractor computed features", keys=list(out.keys()))
        return out