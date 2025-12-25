import pandas as pd
from collections import deque
from typing import Optional
from quant_engine.utils.logger import get_logger, log_debug

class DataCache:
    

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
    def last_timestamp(self) -> int | None:
        """Return timestamp of the most recent bar as epoch milliseconds int."""
        if not self.buffer:
            return None
        latest = self.buffer[-1]
        if "timestamp" not in latest.columns:
            return None
        try:
            return int(latest["timestamp"].iloc[-1])
        except Exception:
            return None

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
    # Timestamp‑aligned queries (v4 unified contract)
    # ------------------------------------------------------------------
    def latest_before_ts(self, ts: int):
        """
        Return the latest bar whose timestamp <= ts (epoch-ms int).
        If no such bar exists, return None.
        """
        if not self.buffer:
            return None

        # Scan from the end — buffer is time‑ordered
        for bar in reversed(self.buffer):
            bar_ts = int(bar["timestamp"].iloc[-1])
            if bar_ts <= int(ts):
                return bar
        return None

    def window_before_ts(self, ts: int, n: int) -> pd.DataFrame:
        """
        Return the last n bars where bar.timestamp <= ts (epoch-ms int).
        Guaranteed anti‑lookahead.
        """
        if not self.buffer:
            return pd.DataFrame()

        valid = []
        for bar in reversed(self.buffer):   # newest → oldest
            bar_ts = int(bar["timestamp"].iloc[-1])
            if bar_ts <= int(ts):
                valid.append(bar)
                if len(valid) == n:
                    break

        if not valid:
            return pd.DataFrame()

        # valid is reversed (newest-first); restore order
        valid.reverse()
        return pd.concat(valid, ignore_index=True)

    def has_ts(self, ts: int) -> bool:
        """
        Return True if the cache contains any bar with timestamp <= ts (epoch-ms int).
        """
        b = self.latest_before_ts(ts)
        return b is not None

    # ------------------------------------------------------------------
    # Detect whether a new bar has arrived
    # ------------------------------------------------------------------
    def has_new_bar(self, prev_ts: int | None) -> bool:
        """
        Return True if latest timestamp differs from prev_ts.
        """
        ts = self.last_timestamp()
        return ts is not None and ts != prev_ts

    def clear(self):
        """Clear all cached bars."""
        log_debug(self._logger, "DataCache cleared")
        self.buffer.clear()