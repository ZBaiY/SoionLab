from collections import deque
from typing import Iterable

from quant_engine.utils.logger import get_logger, log_debug
from quant_engine.data.contracts.cache import SnapshotCache
from quant_engine.data.trades.snapshot import TradesSnapshot


class TradesDataCache(SnapshotCache[TradesSnapshot]):

    def __init__(self, maxlen: int = 10_000):
        self.maxlen = maxlen
        self.buffer = deque(maxlen=maxlen)
        self._logger = get_logger(__name__)
        log_debug(self._logger, "Trades SnapshotCache initialized", maxlen=maxlen)

    def push(self, snapshot: TradesSnapshot) -> None:
        """Add the newest trade snapshot."""
        assert isinstance(snapshot, TradesSnapshot), "Only TradesSnapshot instances are allowed"
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        self.buffer.append(snapshot)

        # deque(maxlen=...) already evicts, this is just defensive
        while len(self.buffer) > self.maxlen:
            self.buffer.popleft()

    def last(self) -> TradesSnapshot | None:
        """Return the most recent snapshot or None if empty."""
        if not self.buffer:
            return None
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"

        return self.buffer[-1]

    def window(self, n: int) -> Iterable[TradesSnapshot]:
        """Return the last n trade snapshots (oldest → newest)."""
        if not self.buffer:
            return []
        return list(self.buffer)[-n:]

    def get_at_or_before(self, timestamp: int) -> TradesSnapshot | None:
        """
        Return the latest trade snapshot whose timestamp <= given timestamp.
        """
        if not self.buffer:
            return None
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"

        for snapshot in reversed(self.buffer):
            if snapshot.data_ts <= timestamp:
                return snapshot
        return None

    def get_n_before(self, timestamp: int, n: int) -> Iterable[TradesSnapshot]:
        """
        Return up to n trade snapshots with timestamp <= given timestamp,
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
        Return True if the cache contains any trade snapshot with timestamp <= ts.
        """
        return self.get_at_or_before(ts) is not None

    def clear(self) -> None:
        """Clear all cached trade snapshots."""
        log_debug(self._logger, "Trades SnapshotCache cleared")
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        self.buffer.clear()