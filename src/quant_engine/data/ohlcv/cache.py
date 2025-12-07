import pandas as pd
from collections import deque
from quant_engine.utils.logger import get_logger, log_debug

class DataCache:
    """
    Rolling window cache for OHLCV or other bar data.
    Keeps the last N bars, used by features and models.
    timestamp | symbol | open | high | low | close | volume
    -------------------------------------------------------
    ...         BTCUSDT
    ...         ETHUSDT
    ...         SOLUSDT
    ----> one single symbol is invested, others are for features, data for models
    ----> this engine is for single symbol only.
    """

    def __init__(self, window: int = 1000):
        self.window = window
        self.buffer = deque(maxlen=window)
        self._logger = get_logger(__name__)
        log_debug(self._logger, "DataCache initialized", window=window)

    def update(self, bar: pd.DataFrame):
        """Add the newest bar (single-row DataFrame)."""
        log_debug(self._logger, "DataCache received bar")
        self.buffer.append(bar)
        log_debug(self._logger, "DataCache updated", size=len(self.buffer))

    def get_window(self, n: int | None = None) -> pd.DataFrame:
        """
        Return rolling window as a DataFrame.
        If n is provided, return only the last n rows.
        """
        log_debug(self._logger, "DataCache returning window", size=len(self.buffer))
        if not self.buffer:
            return pd.DataFrame()

        df = pd.concat(list(self.buffer), ignore_index=True)
        if n is not None:
            return df.tail(n)
        return df

    def get_latest(self) -> pd.DataFrame:
        """Return the most recent bar (single-row DataFrame)."""
        log_debug(self._logger, "DataCache returning latest", size=len(self.buffer))
        if not self.buffer:
            return pd.DataFrame()
        return self.buffer[-1]

    # ------------------------------------------------------------------
    # Latest bar alias
    # ------------------------------------------------------------------
    def latest_bar(self) -> pd.DataFrame:
        """Compatibility wrapper."""
        return self.get_latest()

    # ------------------------------------------------------------------
    # Timestamp of the most recent bar
    # ------------------------------------------------------------------
    def last_timestamp(self):
        """Return timestamp of the most recent bar."""
        if not self.buffer:
            return None
        latest = self.buffer[-1]
        if "timestamp" not in latest.columns:
            return None
        return latest["timestamp"].iloc[-1]

    # ------------------------------------------------------------------
    # Previous close
    # ------------------------------------------------------------------
    def prev_close(self):
        """Return the closing price of the previous bar."""
        if len(self.buffer) < 2:
            return None
        prev = self.buffer[-2]
        if "close" not in prev.columns:
            return None
        return prev["close"].iloc[0]

    # ------------------------------------------------------------------
    # Return last n bars efficiently
    # ------------------------------------------------------------------
    def get_last_n(self, n: int) -> pd.DataFrame:
        """
        Return the last n bars without reconstructing full DataFrame.
        """
        if not self.buffer:
            return pd.DataFrame()

        slice_buf = list(self.buffer)[-n:]
        return pd.concat(slice_buf, ignore_index=True)

    # ------------------------------------------------------------------
    # Detect whether a new bar has arrived
    # ------------------------------------------------------------------
    def has_new_bar(self, prev_ts):
        """
        Return True if latest timestamp differs from prev_ts.
        """
        ts = self.last_timestamp()
        return ts is not None and ts != prev_ts

    def clear(self):
        """Clear all cached bars."""
        log_debug(self._logger, "DataCache cleared")
        self.buffer.clear()