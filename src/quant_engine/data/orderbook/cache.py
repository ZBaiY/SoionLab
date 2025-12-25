import pandas as pd
from collections import deque
from quant_engine.utils.logger import get_logger, log_debug
from typing import Optional

class OrderbookCache:
    """
    Rolling window cache for L1/L2 orderbook snapshots.

    Snapshot structure expected:
    OrderbookSnapshot objects.

    Stored as a deque of OrderbookSnapshot.
    """

    def __init__(self, window: int = 200):
        self.window = window
        self.buffer = deque(maxlen=window)
        self._logger = get_logger(__name__)
        log_debug(self._logger, "OrderbookCache initialized", window=window)

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------
    def update(self, snapshot):
        """Append new L1/L2 snapshot (OrderbookSnapshot)."""
        log_debug(self._logger, "OrderbookCache received snapshot")
        self.buffer.append(snapshot)
        log_debug(self._logger, "OrderbookCache updated", size=len(self.buffer))

    # ------------------------------------------------------------------
    # Latest snapshot
    # ------------------------------------------------------------------
    def get_snapshot(self):
        """Return latest snapshot object."""
        if not self.buffer:
            return None
        return self.buffer[-1]

    # ------------------------------------------------------------------
    # Entire window
    # ------------------------------------------------------------------
    def get_window(self):
        """Return entire window as list of OrderbookSnapshot objects."""
        return list(self.buffer)

    # ------------------------------------------------------------------
    # Convert window to DataFrame
    # ------------------------------------------------------------------
    def window_df(self):
        rows = []
        for snap in self.buffer:
            rows.append({
                "timestamp": snap.timestamp,
                "best_bid": snap.best_bid,
                "best_bid_size": snap.best_bid_size,
                "best_ask": snap.best_ask,
                "best_ask_size": snap.best_ask_size,
            })
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # last n snapshots
    # ------------------------------------------------------------------
    def get_last_n(self, n: int):
        if not self.buffer:
            return []
        return list(self.buffer)[-n:]

    # ------------------------------------------------------------------
    # Timestamp helpers
    # ------------------------------------------------------------------
    def last_timestamp(self) -> int | None:
        snap = self.get_snapshot()
        if snap is None:
            return None
        return int(snap.timestamp)

    def has_new_snapshot(self, prev_ts: int | None) -> bool:
        ts = self.last_timestamp()
        return ts is not None and ts != prev_ts

    # ------------------------------------------------------------------
    # v4 timestamp-aligned queries (anti-lookahead)
    # ------------------------------------------------------------------
    def latest_before_ts(self, ts: int):
        """
        Return the latest OrderbookSnapshot whose snapshot.timestamp <= ts (epoch-ms int).
        If none exists, return None.
        """
        if not self.buffer:
            return None

        # buffer is time-ordered; scan backwards
        for snap in reversed(self.buffer):
            if int(snap.timestamp) <= int(ts):
                return snap
        return None

    def window_before_ts(self, ts: int, n: int):
        """
        Return the last n OrderbookSnapshot objects where snapshot.timestamp <= ts (epoch-ms int).
        Guaranteed anti-lookahead.
        """
        if not self.buffer:
            return []

        valid = []
        for snap in reversed(self.buffer):  # newest -> oldest
            if int(snap.timestamp) <= int(ts):
                valid.append(snap)
                if len(valid) == n:
                    break

        if not valid:
            return []

        valid.reverse()
        return valid

    def has_ts(self, ts: int) -> bool:
        """
        True if cache contains at least one snapshot where timestamp <= ts (epoch-ms int).
        """
        return self.latest_before_ts(ts) is not None

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------
    def clear(self):
        log_debug(self._logger, "OrderbookCache cleared")
        self.buffer.clear()

    def latest(self):
        return self.buffer[-1] if self.buffer else None