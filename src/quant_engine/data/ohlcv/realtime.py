import pandas as pd
from .cache import DataCache
import time
from quant_engine.utils.logger import get_logger, log_debug, log_info
import warnings
from .snapshot import OHLCVSnapshot

class RealTimeDataHandler:
    """
    Real-time handler that receives bars one-by-one (mock or exchange adapter).
    """

    def __init__(self, symbol, window: int = 1000):
        self.symbol = symbol
        self.cache = DataCache(window=window)
        self._logger = get_logger(__name__)
        log_debug(self._logger, "RealTimeDataHandler initialized", window=window)

    @classmethod
    def from_historical(
        cls,
        historical_handler,
        *,
        start_ts: float | pd.Timestamp | None = None,
        window: int = 1000,
    ):
        """
        Build a RealTimeDataHandler seeded from historical data.

        This is used ONLY during backtest initialization to warm up the realtime cache.
        No historical access happens after construction.

        Parameters
        ----------
        historical_handler:
            Source of historical OHLCV data.
        start_ts:
            Backtest start timestamp. If provided, only bars with bar.ts <= start_ts
            are used for warm‑up (anti‑lookahead safe).
        window:
            Cache window size.
        """
        obj = cls(historical_handler.symbol, window=window)

        # Resolve timestamp
        if start_ts is not None:
            ts = pd.Timestamp(start_ts, tz="UTC") if not isinstance(start_ts, pd.Timestamp) else start_ts
            df = historical_handler.window_before_ts(ts, window)
        else:
            # Fallback: last window without timestamp alignment
            log_info(
                obj._logger,
                "RealTimeDataHandler.from_historical: no start_ts provided, using last window",
                symbol=historical_handler.symbol,
            )
            df = historical_handler.window_df(window)

        if df is not None and not df.empty:
            for _, row in df.iterrows():
                obj.cache.update(row.to_frame().T)
        else:
            log_info(
                obj._logger,
                "RealTimeDataHandler.from_historical: no data to seed cache",
                symbol=historical_handler.symbol,
                start_ts=start_ts,
            )
        
        return obj

    def on_new_tick(self, bar: pd.DataFrame):
        """
        Called when a new bar arrives from exchange or websocket.
        """

        log_debug(self._logger, "RealTimeDataHandler received tick")
        self.cache.update(bar)
        log_debug(self._logger, "RealTimeDataHandler updated cache")
        return self.cache.get_window()

    def window_df(self, window: int | None = None):
        """
        Use window(ts, n) with timestamp alignment instead.
        only access for initialization when timestamp is unknown
        """

        df = self.cache.get_window()
        if window is not None:
            return df.tail(window)
        return df

    def last_timestamp(self):
        """
        Return timestamp of the most recent bar.
        """
        df = self.cache.get_window()
        if df is None or df.empty:
            return None
        return df["timestamp"].iloc[-1]

    # ------------------------------------------------------------------
    # v4 unified data access (timestamp‑aligned)
    # ------------------------------------------------------------------
    def get_snapshot(self, ts: float | None = None) -> OHLCVSnapshot | None:
        """
        Return the OHLCVSnapshot for the latest bar with bar.timestamp <= ts.
        Anti‑lookahead: never returns future bars.
        """
        if ts is None:
            ts = self.last_timestamp()
            if ts is None:
                return None
        bar = self.cache.latest_before_ts(ts)
        if bar is None:
            return None


        # bar is a 1‑row DataFrame
        row = bar.iloc[-1]
        return OHLCVSnapshot.from_bar(ts, row)

    def window(self, ts: float | None = None, n: int = 1):
        """
        Return a DataFrame of the last n bars where bar.timestamp <= ts.
        Guaranteed deterministic and anti‑lookahead.
        """
        if ts is None:
            ts = self.last_timestamp()
            if ts is None:
                return pd.DataFrame()
        return self.cache.window_before_ts(ts, n)

    def reset(self):
        log_info(self._logger, "RealTimeDataHandler reset requested")
        try:
            self.cache.clear()
        except AttributeError:
            pass

    def run_mock(self, df: pd.DataFrame, delay=1.0):
        """
        A mock real-time stream for testing without exchange.
        """
        log_info(self._logger, "RealTimeDataHandler starting mock stream", rows=len(df), delay=delay)
        for _, row in df.iterrows():
            bar = row.to_frame().T
            log_debug(self._logger, "RealTimeDataHandler mock tick")
            window = self.on_new_tick(bar)
            yield bar, window
            time.sleep(delay) 