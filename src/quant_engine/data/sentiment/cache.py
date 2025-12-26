from collections import deque
from typing import Iterable

from quant_engine.utils.logger import get_logger, log_debug
from quant_engine.data.contracts.cache import SnapshotCache
from quant_engine.data.sentiment.snapshot import SentimentSnapshot


class SentimentDataCache(SnapshotCache[SentimentSnapshot]):
    """
    Snapshot cache for sentiment events.

    Stores immutable SentimentSnapshot objects in arrival order and
    supports timestamp-aligned lookup. No aggregation or interpretation
    is performed here.
    """

    def __init__(self, maxlen: int = 5_000):
        self.maxlen = maxlen
        self.buffer = deque(maxlen=maxlen)
        self._logger = get_logger(__name__)
        log_debug(self._logger, "Sentiment SnapshotCache initialized", maxlen=maxlen)

    def push(self, snapshot: SentimentSnapshot) -> None:
        """Add the newest sentiment snapshot."""
        assert isinstance(
            snapshot, SentimentSnapshot
        ), "Only SentimentSnapshot instances are allowed"
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        self.buffer.append(snapshot)

    def last(self) -> SentimentSnapshot | None:
        """Return the most recent snapshot or None if empty."""
        if not self.buffer:
            return None
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        return self.buffer[-1]

    def window(self, n: int) -> Iterable[SentimentSnapshot]:
        """Return the last n sentiment snapshots (oldest → newest)."""
        if not self.buffer:
            return []
        return list(self.buffer)[-n:]

    def get_at_or_before(self, timestamp: int) -> SentimentSnapshot | None:
        """
        Return the latest sentiment snapshot whose timestamp <= given timestamp.
        """
        if not self.buffer:
            return None
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        for snapshot in reversed(self.buffer):
            if snapshot.data_ts <= timestamp:
                return snapshot
        return None

    def get_n_before(self, timestamp: int, n: int) -> Iterable[SentimentSnapshot]:
        """
        Return up to n sentiment snapshots with timestamp <= given timestamp,
        ordered oldest to newest.
        """
        if not self.buffer:
            return []

        valid = []
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        for snapshot in reversed(self.buffer):
            if snapshot.data_ts <= timestamp:
                valid.append(snapshot)
                if len(valid) == n:
                    break

        valid.reverse()
        return valid

    def has_ts(self, ts: int) -> bool:
        """
        Return True if the cache contains any sentiment snapshot with timestamp <= ts.
        """
        return self.get_at_or_before(ts) is not None

    def clear(self) -> None:
        """Clear all cached sentiment snapshots."""
        log_debug(self._logger, "Sentiment SnapshotCache cleared")
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        self.buffer.clear()
