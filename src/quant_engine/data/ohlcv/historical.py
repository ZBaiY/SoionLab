import pandas as pd
from .cache import DataCache
from quant_engine.utils.logger import get_logger, log_debug, log_info

class HistoricalDataHandler:
    """
    Load historical OHLCV data, clean it, and feed bars into cache.
    """

    def __init__(self, path: str, window: int = 1000):
        self.path = path
        self.cache = DataCache(window=window)
        self.data = None
        self._logger = get_logger(__name__)

    @classmethod
    def from_dataframe(cls, df, window: int = 1000):
        """
        Construct HistoricalDataHandler directly from a DataFrame.
        Useful for examples, unit tests, and synthetic data.

        Path will be None, and `load()` will not be used.
        """
        obj = cls(path="", window=window)
        obj.data = df.copy()
        obj.cache = DataCache(window=window)
        return obj

    def load(self):
        """Load CSV or Parquet historical data."""
        if self.path == "":
            # Data provided directly; do not load from disk.
            return self.data
        log_debug(self._logger, "HistoricalDataHandler loading file", path=self.path)
        if self.path.endswith(".csv"):
            self.data = pd.read_csv(self.path)
        else:
            raise ValueError("Unsupported file format")

        # ensure timestamps sorted
        self.data = self.data.sort_values("timestamp")
        log_info(self._logger, "HistoricalDataHandler loaded data", rows=len(self.data))

        return self.data

    def iter_bars(self):
        """
        Iterate through historical data one bar at a time.
        Yields each bar as a single-row DataFrame.
        """
        log_debug(self._logger, "HistoricalDataHandler iterating bars")
        if self.data is None:
            self.load()
        assert self.data is not None
        for _, row in self.data.iterrows():
            yield row.to_frame().T

    def stream(self):
        """
        Generator: yield one bar at a time,
        update cache, and return rolling window.
        """
        log_debug(self._logger, "HistoricalDataHandler streaming bars")
        if self.data is None:
            self.load()
            
        assert self.data is not None
        for _, row in self.data.iterrows():
            bar = row.to_frame().T
            self.cache.update(bar)
            log_debug(self._logger, "HistoricalDataHandler updated cache", latest_timestamp=row["timestamp"])
            yield bar, self.cache.get_window()

    # ------------------------------------------------------------------
    # Convenience: return the latest full window DataFrame
    # ------------------------------------------------------------------
    def window_df(self, window: int | None = None):
        """
        Return the latest rolling OHLCV window.
        If `window` is provided, return only the last N rows.
        Otherwise return the DataCache full window.
        """
        df = self.cache.get_window()
        if window is not None:
            return df.tail(window)
        return df

    # ------------------------------------------------------------------
    # Convenience: return the latest bar (1-row DataFrame)
    # ------------------------------------------------------------------
    def latest_bar(self):
        """
        Return the most recent bar as a DataFrame of shape (1, *)
        """
        df = self.cache.get_window()
        if df is None or df.empty:
            return None
        return df.tail(1)

    # ------------------------------------------------------------------
    # Convenience: return the timestamp of the most recent bar
    # ------------------------------------------------------------------
    def last_timestamp(self):
        """
        Return the timestamp of the most recent bar in the cache.
        """
        df = self.cache.get_window()
        if df is None or df.empty:
            return None
        return df["timestamp"].iloc[-1]

    # ------------------------------------------------------------------
    # Convenience: return previous closing price
    # ------------------------------------------------------------------
    def prev_close(self):
        """
        Return closing price of the previous bar.
        Useful for incremental indicators (RSI, ATR, MACD).
        """
        df = self.cache.get_window()
        if df is None or len(df) < 2:
            return None
        return df["close"].iloc[-2]