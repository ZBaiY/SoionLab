from __future__ import annotations
from typing import Optional, Dict, Any, List

from quant_engine.contracts.feature import FeatureChannel
from quant_engine.data.ohlcv.historical import HistoricalDataHandler
from quant_engine.data.ohlcv.realtime import RealTimeDataHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.derivatives.iv.iv_handler import IVSurfaceDataHandler
from quant_engine.data.sentiment.loader import SentimentLoader
from quant_engine.data.orderbook.realtime import RealTimeOrderbookHandler
from quant_engine.data.orderbook.historical import HistoricalOrderbookHandler
from .registry import build_feature
from quant_engine.utils.logger import get_logger, log_debug

min_warmup = 300

class FeatureExtractor:
    """
    TradeBot v4 Unified Feature Extractor

    FeatureChannels in v4 operate **only on timestamp-aligned snapshots**.

    Context passed to each FeatureChannel:
        {
            "ts": float,   # current timestamp
            "data": {
                "ohlcv": {symbol → OHLCVHandler},
                "orderbook": {symbol → OrderbookHandler},
                "options": {symbol → OptionChainHandler},
                "iv_surface": {symbol → IVSurfaceDataHandler},
                "sentiment": {symbol → SentimentHandler},
            },
            "warmup_window": Optional[int],
            "ohlcv_window": Optional[pd.DataFrame],
        }

    FeatureChannels then call:
        snapshot(context, data_type)
        window_any(context, data_type, n)

    No feature should ever access handler internals directly.
    """

    _logger = get_logger(__name__)

    def __init__(
        self,
        ohlcv_handlers: Dict[str, RealTimeDataHandler],
        orderbook_handlers: Dict[str, RealTimeOrderbookHandler],
        option_chain_handlers: Dict[str, OptionChainDataHandler],
        iv_surface_handlers: Dict[str, IVSurfaceDataHandler],
        sentiment_handlers: Dict[str, SentimentLoader],
        feature_config: List[Dict[str, Any]] | None = None,
    ):
        log_debug(self._logger, "Initializing FeatureExtractor")

        self.ohlcv_handlers = ohlcv_handlers
        self.orderbook_handlers = orderbook_handlers
        self.option_chain_handlers = option_chain_handlers
        self.iv_surface_handlers = iv_surface_handlers
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
        # 1) Determine required warmup window across all channels
        max_window = max((ch.required_window() for ch in self.channels), default=1)
        warmup_window = max(max_window, min_warmup)

        # 2) Prefer OHLCV as primary warmup source, but don't assume it exists
        primary_handler: Optional[RealTimeDataHandler] = None
        ohlcv_window = None
        ts_candidates: list[float] = []

        if self.ohlcv_handlers:
            primary_handler = next(iter(self.ohlcv_handlers.values()))
            # warmup OHLCV window (legacy interface, still valid in v4)
            ohlcv_window = primary_handler.window_df(warmup_window)
            if hasattr(primary_handler, "last_timestamp"):
                v = primary_handler.last_timestamp()
                if v is not None:
                    ts_candidates.append(float(v))

        # 3) Harvest timestamps from all other handler families
        def collect_ts(handlers: Dict[str, Any]) -> None:
            for h in handlers.values():
                if hasattr(h, "last_timestamp"):
                    v = h.last_timestamp()
                    if v is not None:
                        ts_candidates.append(float(v))

        collect_ts(self.orderbook_handlers)
        collect_ts(self.option_chain_handlers)
        collect_ts(self.iv_surface_handlers)
        collect_ts(self.sentiment_handlers)

        # 4) Initial logical time = max available timestamp, else 0.0
        ts0 = max(ts_candidates) if ts_candidates else 0.0

        context = {
            "ts": ts0,
            "data": {
                "ohlcv": self.ohlcv_handlers,
                "orderbook": self.orderbook_handlers,
                "options": self.option_chain_handlers,
                "iv_surface": self.iv_surface_handlers,
                "sentiment": self.sentiment_handlers,
            },
            "warmup_window": warmup_window,
            "ohlcv_window": ohlcv_window,
        }

        # 5) Initialize all channels with the same context + warmup_window
        for ch in self.channels:
            ch.initialize(context, warmup_window)

        # 6) Store initial output and internal state
        self._last_output = self.compute_output()
        self._initialized = True
        self._last_ts = ts0

        return self._last_output

    # ----------------------------------------------------------------------
    # Incremental update
    # ----------------------------------------------------------------------
    def update(self, ts: float | None = None) -> Dict[str, Any]:
        """
        Incremental update for new bar arrival.

        Parameters
        ----------
        ts : float | None
            Logical engine timestamp. If None, the extractor will infer
            the timestamp from available handlers (prefer OHLCV when present,
            otherwise fall back to the max last_timestamp across all families).
        """
        # If not initialized → perform warmup
        if not self._initialized:
            return self.initialize()

        # ----------------------------------------------------------
        # 1) Infer ts if not provided
        # ----------------------------------------------------------
        if ts is None:
            candidates: list[float] = []

            # Prefer OHLCV as primary clock if present
            if self.ohlcv_handlers:
                primary = next(iter(self.ohlcv_handlers.values()))
                if hasattr(primary, "last_timestamp"):
                    v = primary.last_timestamp()
                    if v is not None:
                        candidates.append(float(v))

            # Helper to harvest last_timestamp() from other handler families
            def collect_ts(handlers: Dict[str, Any]) -> None:
                for h in handlers.values():
                    if hasattr(h, "last_timestamp"):
                        v = h.last_timestamp()
                        if v is not None:
                            candidates.append(float(v))

            # Other clocks
            collect_ts(self.orderbook_handlers)
            collect_ts(self.option_chain_handlers)
            collect_ts(self.iv_surface_handlers)
            collect_ts(self.sentiment_handlers)

            if candidates:
                # Use the most advanced available timestamp
                ts = max(candidates)
            else:
                # Pure fallback: reuse previous logical time, or 0.0 if none
                ts = self._last_ts if self._last_ts is not None else 0.0

        assert ts is not None

        # ----------------------------------------------------------
        # 2) Anti-lookahead: if time hasn’t advanced, return cached
        # ----------------------------------------------------------
        if self._last_ts is not None and ts <= self._last_ts:
            return self._last_output

        # ----------------------------------------------------------
        # 3) Build context & update channels
        # ----------------------------------------------------------
        context = {
            "ts": ts,
            "data": {
                "ohlcv": self.ohlcv_handlers,
                "orderbook": self.orderbook_handlers,
                "options": self.option_chain_handlers,
                "iv_surface": self.iv_surface_handlers,
                "sentiment": self.sentiment_handlers,
            },
        }

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