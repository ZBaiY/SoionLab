import time
from quant_engine.data.orderbook.cache import OrderbookCache
from quant_engine.data.orderbook.snapshot import OrderbookSnapshot
from quant_engine.utils.logger import get_logger, log_debug, log_info
from quant_engine.data.orderbook.historical import HistoricalOrderbookHandler
import warnings

class SentimentHandler:
    """
    Real-time L1/L2 orderbook handler.
    Receives snapshots from:
        - websocket
        - mock streams (for backtest simulation)
        - matching engine feedback (live trading)

    Mirrors the structure of RealTimeDataHandler (OHLCV),
    but operates on OrderbookSnapshot objects instead of DataFrames.
    """

    def __init__(self, symbol: str, window: int = 200):
        self.symbol = symbol
        self.cache = OrderbookCache(window=window)
        self._logger = get_logger(__name__)
        log_debug(self._logger, "RealTimeOrderbookHandler initialized", symbol=symbol, window=window)
    
    @classmethod
    def from_historical(cls, historical_handler: HistoricalOrderbookHandler):
        """
        Build a RealTimeOrderbookHandler from a HistoricalOrderbookHandler.
        Preloads existing historical snapshots into realtime cache.
        """
        rt = cls(
            symbol=historical_handler.symbol,
            window=historical_handler.cache.window,
        )
        for snapshot in historical_handler.cache.get_window():
            rt.cache.update(snapshot)
        return rt
    
    # ------------------------------------------------------------------
    def on_new_snapshot(self, snapshot: OrderbookSnapshot):
        """
        Push a new orderbook snapshot (from exchange or mock source).
        """
        log_debug(self._logger, "RealTimeOrderbookHandler received snapshot")
        self.cache.update(snapshot)
        log_debug(self._logger, "RealTimeOrderbookHandler cache updated")
        return self.cache.get_window()

    # ------------------------------------------------------------------
    def latest_snapshot(self):
        """
        [DEPRECATED — v4]
        Use get_snapshot(ts) instead.
        """
        warnings.warn(
            "RealTimeOrderbookHandler.latest_snapshot() is deprecated in v4.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.cache.latest()

    # ------------------------------------------------------------------
    def last_timestamp(self):
        """Return timestamp of the most recent snapshot."""
        snap = self.cache.latest()
        if snap is None:
            return None
        return snap.timestamp

    # ------------------------------------------------------------------
    # v4 timestamp-aligned data access
    # ------------------------------------------------------------------
    def get_snapshot(self, ts: float):
        """
        Return the latest OrderbookSnapshot whose timestamp <= ts.
        This is the core anti-lookahead alignment mechanism.
        """
        snap = self.cache.latest_before_ts(ts)
        if snap is None:
            return None
        return snap

    # ------------------------------------------------------------------
    def window(self, ts: float | None = None, n: int | None = None):
        """
        v4 unified rolling window selector.

        If ts is given:
            Return all snapshots where timestamp <= ts,
            limited to the most recent n items if n is provided.

        If ts is None (legacy mode):
            Behave like old handler.window(n).
        """
        
        if ts is None:
            warnings.warn(
                "RealTimeOrderbookHandler.window(ts=None) legacy mode is deprecated in v4. "
                "Use window(ts, n) with timestamp alignment.",
                DeprecationWarning,
                stacklevel=2,
            )
            # Legacy mode (no timestamp alignment)
            window = self.cache.get_window()
            if n is not None:
                return window[-n:]
            return window

        # v4 timestamp-aligned mode
        if n is None:
            # Return all <= ts
            snaps = self.cache.window_before_ts(ts, len(self.cache.get_window()))
            return snaps
        else:
            return self.cache.window_before_ts(ts, n)

    # ------------------------------------------------------------------
    def reset(self):
        log_info(self._logger, "RealTimeOrderbookHandler reset requested")
        self.cache.clear()

    # ------------------------------------------------------------------
    def run_mock(self, df, delay: float = 0.0):
        """
        v4-compliant simulated orderbook stream.
        - df: DataFrame containing timestamp, best_bid, best_ask, bids, asks
        - Converts each row into an OrderbookSnapshot
        - Feeds snapshots through the normal realtime ingestion pipeline

        NOTE:
        delay is optional and defaults to 0 for fast backtest-style replay.
        """
        log_info(
            self._logger,
            "RealTimeOrderbookHandler starting v4 mock stream",
            rows=len(df),
            delay=delay,
        )

        for _, row in df.iterrows():
            # Convert DataFrame row → snapshot
            raw = row.to_dict()

            snapshot = OrderbookSnapshot(
                symbol=self.symbol,
                timestamp=float(raw["timestamp"]),
                best_bid=float(raw["best_bid"]),
                best_bid_size=float(raw["best_bid_size"]),
                best_ask=float(raw["best_ask"]),
                best_ask_size=float(raw["best_ask_size"]),
                bids=raw.get("bids", []),
                asks=raw.get("asks", []),
                latency=0.0,      # mock streams define no real latency
            )

            # Inject snapshot into realtime handler (cache update)
            window = self.on_new_snapshot(snapshot)

            # Yield for test harness
            yield snapshot, window

            # Optional delay for real-time simulation
            if delay > 0:
                time.sleep(delay)
