from collections import deque
from typing import Iterable
from quant_engine.utils.logger import get_logger, log_debug
from quant_engine.data.contracts.cache import SnapshotCache
from quant_engine.data.ohlcv.snapshot import OHLCVSnapshot

class OHLCVDataCache(SnapshotCache[OHLCVSnapshot]):
    

    def __init__(self, maxlen: int = 1000):
        self.maxlen = maxlen
        self.buffer = deque(maxlen=maxlen)
        self._logger = get_logger(__name__)
        log_debug(self._logger, "SnapshotCache initialized", maxlen=maxlen)

    def push(self, snapshot: OHLCVSnapshot) -> None:
        """Add the newest snapshot."""
        assert isinstance(snapshot, OHLCVSnapshot), "Only OHLCVSnapshot instances are allowed"
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        self.buffer.append(snapshot)
        while len(self.buffer) > self.maxlen:
            self.buffer.popleft()

    def last(self) -> OHLCVSnapshot | None:
        """Return the most recent snapshot or None if empty."""
        if not self.buffer:
            return None
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        return self.buffer[-1]

    def window(self, n: int) -> Iterable[OHLCVSnapshot]:
        """Return the last n snapshots as an iterable."""
        if not self.buffer:
            return []
        return list(self.buffer)[-n:]

    def get_at_or_before(self, timestamp: int) -> OHLCVSnapshot | None:
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

    def get_n_before(self, timestamp: int, n: int) -> list[OHLCVSnapshot]:
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

    def clear(self):
        """Clear all cached snapshots."""
        log_debug(self._logger, "SnapshotCache cleared")
        assert isinstance(self.buffer, deque), "Internal buffer must be a deque"
        self.buffer.clear()