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

min_warmup = 300

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
        ohlcv_handlers: Dict[str, RealTimeDataHandler],
        orderbook_handlers: Dict[str, RealTimeOrderbookHandler],
        option_chain_handlers: Dict[str, OptionChainDataHandler],
        sentiment_handlers: Dict[str, SentimentLoader],
        feature_config: List[Dict[str, Any]] | None = None,
    ):
        log_debug(self._logger, "Initializing FeatureExtractor")

        self.ohlcv_handlers = ohlcv_handlers
        self.orderbook_handlers = orderbook_handlers
        self.option_chain_handlers = option_chain_handlers
        self.sentiment_handlers = sentiment_handlers

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

        # Industry-standard minimum warmup (stability for RSI, ATR, MACD, ZScore, etc.)
        
        warmup_window = max(max_window, min_warmup)

        primary_handler = next(iter(self.ohlcv_handlers.values()))
        ohlcv_window = primary_handler.window_df(warmup_window)

        context = {
            "ohlcv_handlers": self.ohlcv_handlers,
            "orderbook_handlers": self.orderbook_handlers,
            "option_chain_handlers": self.option_chain_handlers,
            "sentiment_handlers": self.sentiment_handlers,
            "ohlcv_window": ohlcv_window,
        }

        # initialize all channels
        for ch in self.channels:
            ch.initialize(context, warmup_window)

        # store initial output
        self._last_output = self.compute_output()
        self._initialized = True
        self._last_ts = primary_handler.last_timestamp()

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

        primary_handler = next(iter(self.ohlcv_handlers.values()))
        ts = primary_handler.last_timestamp()
        if ts == self._last_ts:
            return self._last_output   # no new bar

        # Collect latest bars for ALL symbols.
        latest_bars = {}
        for sym, h in self.ohlcv_handlers.items():
            latest_bars[sym] = h.latest_bar()

        context = {
            # Dict[str, DataFrame] — each feature will extract the correct symbol
            "ohlcv": latest_bars,
            "ohlcv_handlers": self.ohlcv_handlers,
            "orderbook_handlers": self.orderbook_handlers,
            "option_chain_handlers": self.option_chain_handlers,
            "sentiment_handlers": self.sentiment_handlers,
        }

        # incremental update
        for ch in self.channels:
            ch.update(context)

        self._last_output = self.compute_output()
        self._last_ts = primary_handler.last_timestamp()
        return self._last_output

    # ----------------------------------------------------------------------
    # Output aggregator
    # ----------------------------------------------------------------------
    def compute_output(self) -> Dict[str, Any]:
        """
        Collect feature outputs from all channels and standardize keys as:

            TYPE_SYMBOL
            TYPE_REF^SYMBOL

        Example:
            {"RSI_BTCUSDT": 56.2}
            {"SPREAD_BTCUSDT^ETHUSDT": 0.014}

        Assumes each FeatureChannel exposes:
            - ch.symbol: primary symbol
            - ch.params: dictionary which may include "ref"
            - ch.output(): returns dict of {raw_key: value}
        """
        result: Dict[str, Any] = {}

        for ch in self.channels:
            raw = ch.output()
            symbol = getattr(ch, "symbol", None)
            params = getattr(ch, "params", {}) or {}
            ref = params.get("ref")

            for k, v in raw.items():
                # Base name: TYPE
                base = k.upper()

                # Attach primary symbol
                if ref and symbol:
                    base = f"{base}_{ref}^{symbol}"
                elif symbol:
                    base = f"{base}_{symbol}"

                # Attach reference symbol if exists
                

                result[base] = v

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