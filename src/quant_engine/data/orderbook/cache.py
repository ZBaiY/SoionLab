import pandas as pd
from collections import deque
from quant_engine.data.orderbook.snapshot import OrderbookSnapshot
from quant_engine.utils.logger import get_logger, log_debug
from quant_engine.data.contracts.cache import SnapshotCache
from typing import Iterable, Optional

class OrderbookCache(SnapshotCache):
    """
    Rolling window cache for L1/L2 orderbook snapshots.

    Snapshot structure expected:
    OrderbookSnapshot objects.

    Stored as a deque of OrderbookSnapshot.
    """

    def __init__(self, maxlen: int = 200):
        self.maxlen = maxlen
        self.buffer = deque(maxlen=maxlen)
        self._logger = get_logger(__name__)
        log_debug(self._logger, "OrderbookCache initialized", maxlen=maxlen)
    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------
    def push(self, snapshot):
        """Append new L1/L2 snapshot (OrderbookSnapshot)."""
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        self.buffer.append(snapshot)
        log_debug(self._logger, "OrderbookCache updated", size=len(self.buffer))

    # ------------------------------------------------------------------
    # Latest snapshot
    # ------------------------------------------------------------------
    def get_snapshot(self):
        """Return latest snapshot object."""
        if not self.buffer:
            return None
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        return self.buffer[-1]

    # ------------------------------------------------------------------
    # Convert window to DataFrame
    # ------------------------------------------------------------------
    def window_df(self):
        rows = []
        for snap in self.buffer:
            rows.append({
                "timestamp": snap.data_ts,
                "best_bid": snap.best_bid,
                "best_bid_size": snap.best_bid_size,
                "best_ask": snap.best_ask,
                "best_ask_size": snap.best_ask_size,
            })
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Timestamp helpers
    # ------------------------------------------------------------------
    def last_timestamp(self) -> int | None:
        snap = self.get_snapshot()
        if snap is None:
            return None
        return int(snap.data_ts)

    def has_new_snapshot(self, prev_ts: int | None) -> bool:
        ts = self.last_timestamp()
        return ts is not None and ts != prev_ts

    # ------------------------------------------------------------------
    # v4 timestamp-aligned queries (anti-lookahead)
    # ------------------------------------------------------------------
    def window(self, n: int) -> Iterable[OrderbookSnapshot]:
        """Return the last n snapshots as an iterable."""
        if not self.buffer:
            return []
        return list(self.buffer)[-n:]

    def get_at_or_before(self, timestamp: int) -> OrderbookSnapshot | None:
        """
        Return the latest snapshot whose timestamp <= given timestamp.
        If none found, return None.
        """
        if not self.buffer:
            return None
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        for snapshot in reversed(self.buffer):
            if snapshot.data_ts <= timestamp:
                return snapshot
        return None

    def get_n_before(self, timestamp: int, n: int) -> Iterable[OrderbookSnapshot]:
        """
        Return up to n snapshots with timestamp <= given timestamp,
        ordered oldest to newest.
        """
        if not self.buffer:
            return []
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        valid = []
        for snapshot in reversed(self.buffer):
            if snapshot.data_ts <= timestamp:
                valid.append(snapshot)
                if len(valid) == n:
                    break
        valid.reverse()
        return valid
    def has_ts(self, ts: int) -> bool:
        """
        Return True if the cache contains any snapshot with timestamp <= ts.
        """
        return self.get_at_or_before(ts) is not None
    
    def last(self) -> OrderbookSnapshot | None:
        """Return the most recent snapshot or None if empty."""
        if not self.buffer:
            return None
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        return self.buffer[-1]
    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------
    def clear(self):
        log_debug(self._logger, "OrderbookCache cleared")
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        self.buffer.clear()

    def latest(self):
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        return self.buffer[-1] if self.buffer else None