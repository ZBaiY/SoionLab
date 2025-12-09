import pandas as pd
from .cache import DataCache
import time
from quant_engine.utils.logger import get_logger, log_debug, log_info

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
    def from_historical(cls, historical_handler, window: int = 1000):
        """
        Build a RealTimeDataHandler seeded with historical data.
        Use case: backtesting where historical bars are preloaded into realtime cache.
        """
        obj = cls(historical_handler.symbol, window=window)
        # preload historical window into realtime cache
        df = historical_handler.window_df(window)
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                obj.cache.update(row.to_frame().T)
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
        Return the latest rolling OHLCV window.
        If `window` is provided, return only the last N rows.
        Otherwise return full window.
        """
        df = self.cache.get_window()
        if window is not None:
            return df.tail(window)
        return df

    def latest_bar(self):
        """
        Return the most recent bar as a DataFrame of shape (1, *)
        """
        log_debug(self._logger, "RealTimeDataHandler retrieving latest bar")
        df = self.cache.get_window()
        if df is None or df.empty:
            return None
        return df.tail(1)

    def latest_tick(self):
        # Backward compatibility
        return self.latest_bar()

    def last_timestamp(self):
        """
        Return timestamp of the most recent bar.
        """
        df = self.cache.get_window()
        if df is None or df.empty:
            return None
        return df["timestamp"].iloc[-1]

    def prev_close(self):
        """
        Return closing price of the previous bar.
        """
        df = self.cache.get_window()
        if df is None or len(df) < 2:
            return None
        return df["close"].iloc[-2]

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