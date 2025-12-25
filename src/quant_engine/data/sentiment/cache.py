from collections import deque
from typing import Deque
from quant_engine.data.sentiment.snapshot import SentimentSnapshot


class SentimentCache:
    """Small timestamp-aligned cache for SentimentSnapshot."""

    def __init__(self, max_bars: int):
        self._dq: Deque[SentimentSnapshot] = deque(maxlen=int(max_bars))

    def clear(self) -> None:
        self._dq.clear()

    def update(self, snap: SentimentSnapshot) -> None:
        if not self._dq or snap.timestamp >= self._dq[-1].timestamp:
            self._dq.append(snap)
            return
        items = list(self._dq)
        items.append(snap)
        items.sort(key=lambda s: s.timestamp)
        assert self._dq.maxlen is not None
        self._dq = deque(items[-self._dq.maxlen :], maxlen=self._dq.maxlen)

    def latest(self) -> SentimentSnapshot | None:
        return self._dq[-1] if self._dq else None

    def latest_before_ts(self, ts: int) -> SentimentSnapshot | None:
        if not self._dq:
            return None
        for s in reversed(self._dq):
            if int(s.timestamp) <= int(ts):
                return s
        return None

    def window_before_ts(self, ts: int, n: int) -> list[SentimentSnapshot]:
        if not self._dq:
            return []
        out: list[SentimentSnapshot] = []
        for s in reversed(self._dq):
            if int(s.timestamp) <= int(ts):
                out.append(s)
                if len(out) >= n:
                    break
        out.reverse()
        return out
