from __future__ import annotations

from collections import deque
from typing import Iterable, TypeVar, runtime_checkable, Generic

from quant_engine.data.contracts.cache import SnapshotCache
from quant_engine.data.derivatives.option_trades.snapshot import OptionTradeEvent

EvT = TypeVar("EvT", bound=OptionTradeEvent)


def _event_ts(e: object) -> int:
    """Best-effort event-time accessor.

    Supports both naming conventions you have used in the codebase:
    - e.data_ts
    - e.event_ts
    """
    ts = getattr(e, "data_ts", None)
    if ts is None:
        ts = getattr(e, "event_ts", None)
    if ts is None:
        raise AttributeError("Event object must expose data_ts or event_ts")
    return int(ts)


def _expiry_ts(e: object) -> int:
    ts = getattr(e, "expiry_ts", None)
    if ts is None:
        raise AttributeError("OptionTradeEvent must expose expiry_ts")
    return int(ts)


def _term_key_ms(e: object, *, term_bucket_ms: int) -> int:
    """Bucket key for term-structure queries.

    term := expiry_ts - event_ts (ms)
    key  := floor(term / term_bucket_ms) * term_bucket_ms
    """
    tb = int(term_bucket_ms)
    if tb <= 0:
        raise ValueError("term_bucket_ms must be > 0")
    term = _expiry_ts(e) - _event_ts(e)
    if term < 0:
        term = 0
    return (int(term) // tb) * tb


class DequeEventCache(Generic[EvT], SnapshotCache[EvT]):
    """Simple deque-backed cache ordered by timestamp (append order).

    push(): O(1)
    get_at_or_before(): O(N) reverse scan (good enough for moderate cache sizes)
    """

    def __init__(self, maxlen: int = 1000):
        self.maxlen = int(maxlen)
        self.buffer: deque[EvT] = deque(maxlen=self.maxlen)

    def push(self, e: EvT) -> None:
        self.buffer.append(e)

    def last(self) -> EvT | None:
        return self.buffer[-1] if self.buffer else None

    def window(self, n: int) -> Iterable[EvT]:
        if not self.buffer:
            return []
        k = int(n)
        return list(self.buffer)[-k:]

    def get_at_or_before(self, timestamp: int) -> EvT | None:
        t = int(timestamp)
        for e in reversed(self.buffer):
            if _event_ts(e) <= t:
                return e
        return None

    def get_n_before(self, timestamp: int, n: int) -> Iterable[EvT]:
        t = int(timestamp)
        k = int(n)
        out: list[EvT] = []
        for e in reversed(self.buffer):
            if _event_ts(e) <= t:
                out.append(e)
                if len(out) == k:
                    break
        out.reverse()
        return out

    def has_ts(self, ts: int) -> bool:
        return self.get_at_or_before(int(ts)) is not None

    def clear(self) -> None:
        self.buffer.clear()


class OptionTradesTermBucketedCache(SnapshotCache[OptionTradeEvent]):
    """Multi-index cache for option trades.

    Indices:
      1) main: global time-ordered cache (engine alignment / replay / windowing)
      2) by_term: per-term-bucket caches, where term := expiry_ts - event_ts
         (term structure / DTE-bucket features)
      3) by_expiry: per-expiry caches (optional compatibility helper)

    IMPORTANT:
    - main ordering is ALWAYS by event timestamp.
    - expiry/term are indexing/bucketing only; never the sort key.
    """

    def __init__(
        self,
        *,
        maxlen: int = 200_000,
        per_term_maxlen: int = 50_000,
        term_bucket_ms: int = 86_400_000,
        # deprecated name (kept for compatibility)
        per_expiry_maxlen: int | None = None,
    ):
        self.maxlen = int(maxlen)
        self.term_bucket_ms = int(term_bucket_ms)
        if self.maxlen <= 0:
            raise ValueError("maxlen must be > 0")
        if self.term_bucket_ms <= 0:
            raise ValueError("term_bucket_ms must be > 0")

        self._per_term_maxlen = int(per_term_maxlen)
        if self._per_term_maxlen <= 0:
            raise ValueError("per_term_maxlen must be > 0")

        # If a caller still passes per_expiry_maxlen explicitly, honor it; otherwise default to per_term_maxlen.
        self._per_expiry_maxlen = int(per_expiry_maxlen) if per_expiry_maxlen is not None else int(self._per_term_maxlen)
        if self._per_expiry_maxlen <= 0:
            raise ValueError("per_expiry_maxlen must be > 0")

        self.main: DequeEventCache[OptionTradeEvent] = DequeEventCache(maxlen=self.maxlen)

        # Indices
        self.by_term: dict[int, DequeEventCache[OptionTradeEvent]] = {}
        self.by_expiry: dict[int, DequeEventCache[OptionTradeEvent]] = {}

        # Protocol attribute (iterable buffer) points to the global time cache.
        self.buffer = self.main.buffer

    def _bucket_term(self, term_key_ms: int) -> DequeEventCache[OptionTradeEvent]:
        k = int(term_key_ms)
        b = self.by_term.get(k)
        if b is None:
            b = DequeEventCache(maxlen=self._per_term_maxlen)
            self.by_term[k] = b
        return b

    def _bucket_expiry(self, expiry_ts: int) -> DequeEventCache[OptionTradeEvent]:
        k = int(expiry_ts)
        b = self.by_expiry.get(k)
        if b is None:
            b = DequeEventCache(maxlen=self._per_expiry_maxlen)
            self.by_expiry[k] = b
        return b

    # --- SnapshotCache interface (delegates to main) ---

    def push(self, e: OptionTradeEvent) -> None:
        self.main.push(e)
        # term-bucket index
        tk = _term_key_ms(e, term_bucket_ms=self.term_bucket_ms)
        self._bucket_term(tk).push(e)
        # optional expiry index
        self._bucket_expiry(_expiry_ts(e)).push(e)

    def last(self) -> OptionTradeEvent | None:
        return self.main.last()

    def window(self, n: int) -> Iterable[OptionTradeEvent]:
        return self.main.window(n)

    def get_at_or_before(self, timestamp: int) -> OptionTradeEvent | None:
        return self.main.get_at_or_before(timestamp)

    def get_n_before(self, timestamp: int, n: int) -> Iterable[OptionTradeEvent]:
        return self.main.get_n_before(timestamp, n)

    def has_ts(self, ts: int) -> bool:
        return self.get_at_or_before(int(ts)) is not None

    def clear(self) -> None:
        self.main.clear()
        for b in self.by_term.values():
            b.clear()
        self.by_term.clear()
        for b in self.by_expiry.values():
            b.clear()
        self.by_expiry.clear()

    # --- Extra helpers (term buckets) ---

    def term_buckets(self) -> list[int]:
        return sorted(self.by_term.keys())

    # Back-compat: some callers may still use `expiries()` name.
    # In the term-bucketed design, this returns term bucket keys.
    def expiries(self) -> list[int]:
        return self.term_buckets()

    def last_for_term(self, term_key_ms: int) -> OptionTradeEvent | None:
        b = self.by_term.get(int(term_key_ms))
        return b.last() if b is not None else None

    def window_for_term(self, term_key_ms: int, n: int) -> Iterable[OptionTradeEvent]:
        b = self.by_term.get(int(term_key_ms))
        return b.window(n) if b is not None else []

    def get_at_or_before_for_term(self, term_key_ms: int, timestamp: int) -> OptionTradeEvent | None:
        b = self.by_term.get(int(term_key_ms))
        return b.get_at_or_before(timestamp) if b is not None else None

    def get_n_before_for_term(self, term_key_ms: int, timestamp: int, n: int) -> Iterable[OptionTradeEvent]:
        b = self.by_term.get(int(term_key_ms))
        return b.get_n_before(timestamp, n) if b is not None else []

    # --- Extra helpers (per-expiry) ---

    def expiry_list(self) -> list[int]:
        return sorted(self.by_expiry.keys())

    def last_for_expiry(self, expiry_ts: int) -> OptionTradeEvent | None:
        b = self.by_expiry.get(int(expiry_ts))
        return b.last() if b is not None else None

    def window_for_expiry(self, expiry_ts: int, n: int) -> Iterable[OptionTradeEvent]:
        b = self.by_expiry.get(int(expiry_ts))
        return b.window(n) if b is not None else []

    def get_at_or_before_for_expiry(self, expiry_ts: int, timestamp: int) -> OptionTradeEvent | None:
        b = self.by_expiry.get(int(expiry_ts))
        return b.get_at_or_before(timestamp) if b is not None else None

    def get_n_before_for_expiry(self, expiry_ts: int, timestamp: int, n: int) -> Iterable[OptionTradeEvent]:
        b = self.by_expiry.get(int(expiry_ts))
        return b.get_n_before(timestamp, n) if b is not None else []


OptionTradesBucketedCache = OptionTradesTermBucketedCache