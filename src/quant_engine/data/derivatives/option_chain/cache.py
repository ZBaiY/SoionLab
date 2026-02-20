from __future__ import annotations

"""Option-chain snapshot caches.

Design goals:
- Main ordering is ALWAYS by snapshot event-time (snapshot.data_ts).
- Expiry is an indexing key only; it must never become the primary sort key.
- Term / DTE bucketing is query-time (term := expiry_ts - ts), not an ingest-time index.

This module intentionally stores references to snapshots in secondary indices
(e.g. per-expiry buckets) to avoid duplicating large record payloads.
"""

from collections import deque
from typing import Generic, Iterable, TypeAlias, TypeVar

import pandas as pd

from quant_engine.data.contracts.cache import SnapshotCache
from quant_engine.data.derivatives.option_chain.snapshot import OptionChainSnapshot, OptionChainSnapshotView


SnapT = TypeVar("SnapT", bound=OptionChainSnapshot)


def _snap_ts(s: object) -> int:
    ts = getattr(s, "data_ts", None)
    if ts is None:
        ts = getattr(s, "timestamp", None)
    if ts is None:
        raise AttributeError("Snapshot object must expose data_ts (preferred) or timestamp")
    return int(ts)


# Note: expiry keys are cached on OptionChainSnapshot via get_expiry_keys_ms().


def _term_key_ms(*, snap_ts: int, expiry_ts: int, term_bucket_ms: int) -> int:
    """Bucket key for term-structure queries.

    term := expiry_ts - snap_ts (ms)
    key  := floor(term / term_bucket_ms) * term_bucket_ms
    """
    tb = int(term_bucket_ms)
    if tb <= 0:
        raise ValueError("term_bucket_ms must be > 0")
    term = int(expiry_ts) - int(snap_ts)
    if term < 0:
        term = 0
    return (int(term) // tb) * tb


def _concat_frames(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    xs = [f for f in frames if f is not None]
    if not xs:
        return pd.DataFrame()
    return pd.concat(xs, ignore_index=True)


class DequeSnapshotCache(Generic[SnapT], SnapshotCache[SnapT]):
    """Deque-backed cache ordered by snapshot timestamp (append order).

    push(): O(1)
    get_at_or_before(): O(N) reverse scan (OK for moderate N)
    """

    def __init__(self, maxlen: int = 512):
        self.maxlen = int(maxlen)
        if self.maxlen <= 0:
            raise ValueError("maxlen must be > 0")
        self.buffer: deque[SnapT] = deque(maxlen=self.maxlen)

    def push(self, s: SnapT) -> SnapT | None:
        evicted = self.buffer[0] if len(self.buffer) == self.maxlen else None
        self.buffer.append(s)
        return evicted

    def last(self) -> SnapT | None:
        return self.buffer[-1] if self.buffer else None

    def window(self, n: int) -> Iterable[SnapT]:
        if not self.buffer:
            return []
        k = int(n)
        return list(self.buffer)[-k:]

    def get_at_or_before(self, timestamp: int) -> SnapT | None:
        t = int(timestamp)
        for s in reversed(self.buffer):
            if _snap_ts(s) <= t:
                return s
        return None

    def get_n_before(self, timestamp: int, n: int) -> Iterable[SnapT]:
        t = int(timestamp)
        k = int(n)
        out: list[SnapT] = []
        for s in reversed(self.buffer):
            if _snap_ts(s) <= t:
                out.append(s)
                if len(out) == k:
                    break
        out.reverse()
        return out

    def has_ts(self, ts: int) -> bool:
        return self.get_at_or_before(int(ts)) is not None

    def clear(self) -> None:
        self.buffer.clear()


class OptionChainExpiryIndexedCache(SnapshotCache[OptionChainSnapshot]):
    """Option-chain cache with expiry-sliced view helpers.

    Assumes snapshots are pushed in non-decreasing data_ts order; out-of-order pushes are undefined behavior.
    """

    def __init__(
        self,
        *,
        maxlen: int = 512,
        default_expiry_window: int = 5,
    ):
        self.maxlen = int(maxlen)
        self.default_expiry_window = int(default_expiry_window)
        if self.maxlen <= 0:
            raise ValueError("maxlen must be > 0")
        if self.default_expiry_window <= 0:
            raise ValueError("default_expiry_window must be > 0")

        self.main: DequeSnapshotCache[OptionChainSnapshot] = DequeSnapshotCache(maxlen=self.maxlen)

        # Protocol attribute: iterable buffer points to global cache.
        self.buffer = self.main.buffer

    # --- SnapshotCache interface (delegates to main) ---

    def push(self, s: OptionChainSnapshot) -> OptionChainSnapshot | None:
        return self.main.push(s)

    def last(self) -> OptionChainSnapshot | None:
        return self.main.last()

    def window(self, n: int) -> Iterable[OptionChainSnapshot]:
        return self.main.window(n)

    def get_at_or_before(self, timestamp: int) -> OptionChainSnapshot | None:
        return self.main.get_at_or_before(timestamp)

    def get_n_before(self, timestamp: int, n: int) -> Iterable[OptionChainSnapshot]:
        return self.main.get_n_before(timestamp, n)

    def has_ts(self, ts: int) -> bool:
        return self.main.has_ts(ts)

    def clear(self) -> None:
        self.main.clear()

    # --- Expiry helpers ---

    def expiries(self) -> list[int]:
        s = self.last()
        if s is None:
            return []
        return sorted(s.get_expiry_keys_ms())

    def last_for_expiry(self, expiry_ts: int) -> OptionChainSnapshot | None:
        base = self.main.last()
        return None if base is None else OptionChainSnapshotView.for_expiry(base=base, expiry_ts=expiry_ts)

    def get_at_or_before_for_expiry(self, expiry_ts: int, timestamp: int) -> OptionChainSnapshot | None:
        base = self.main.get_at_or_before(timestamp)
        return None if base is None else OptionChainSnapshotView.for_expiry(base=base, expiry_ts=expiry_ts)

    def window_for_expiry(self, expiry_ts: int, n: int) -> Iterable[OptionChainSnapshot]:
        out: list[OptionChainSnapshot] = []
        for base in self.main.window(n):
            out.append(OptionChainSnapshotView.for_expiry(base=base, expiry_ts=expiry_ts))
        return out

    def get_n_before_for_expiry(self, expiry_ts: int, timestamp: int, n: int) -> Iterable[OptionChainSnapshot]:
        out: list[OptionChainSnapshot] = []
        for base in self.main.get_n_before(timestamp, n):
            out.append(OptionChainSnapshotView.for_expiry(base=base, expiry_ts=expiry_ts))
        return out

    def window_df_for_expiry(self, expiry_ts: int, n: int | None = None) -> pd.DataFrame:
        k = self.default_expiry_window if n is None else int(n)
        return _concat_frames([s.frame for s in self.window_for_expiry(expiry_ts, k)])

    def get_n_before_df_for_expiry(self, expiry_ts: int, timestamp: int, n: int | None = None) -> pd.DataFrame:
        k = self.default_expiry_window if n is None else int(n)
        return _concat_frames([s.frame for s in self.get_n_before_for_expiry(expiry_ts, timestamp, k)])


class OptionChainTermBucketedCache(SnapshotCache[OptionChainSnapshot]):
    """Option-chain cache with term-bucket view helpers.

    Assumes snapshots are pushed in non-decreasing data_ts order; out-of-order pushes are undefined behavior.
    """

    def __init__(
        self,
        *,
        maxlen: int = 512,
        term_bucket_ms: int = 86_400_000,
        default_term_window: int = 5,
        default_expiry_window: int = 5,
    ):
        self.maxlen = int(maxlen)
        self.term_bucket_ms = int(term_bucket_ms)
        self.default_term_window = int(default_term_window)
        self.default_expiry_window = int(default_expiry_window)
        if self.maxlen <= 0:
            raise ValueError("maxlen must be > 0")
        if self.term_bucket_ms <= 0:
            raise ValueError("term_bucket_ms must be > 0")
        if self.default_term_window <= 0:
            raise ValueError("default_term_window must be > 0")
        if self.default_expiry_window <= 0:
            raise ValueError("default_expiry_window must be > 0")

        self.main: DequeSnapshotCache[OptionChainSnapshot] = DequeSnapshotCache(maxlen=self.maxlen)

        # Protocol attribute: iterable buffer points to global cache.
        self.buffer = self.main.buffer

    # --- SnapshotCache interface (delegates to main) ---

    def push(self, s: OptionChainSnapshot) -> OptionChainSnapshot | None:
        evicted = self.main.push(s)
        s.get_term_keys_ms(self.term_bucket_ms)
        return evicted

    def last(self) -> OptionChainSnapshot | None:
        return self.main.last()

    def window(self, n: int) -> Iterable[OptionChainSnapshot]:
        return self.main.window(n)

    def get_at_or_before(self, timestamp: int) -> OptionChainSnapshot | None:
        return self.main.get_at_or_before(timestamp)

    def get_n_before(self, timestamp: int, n: int) -> Iterable[OptionChainSnapshot]:
        return self.main.get_n_before(timestamp, n)

    def has_ts(self, ts: int) -> bool:
        return self.main.has_ts(ts)

    def clear(self) -> None:
        self.main.clear()

    # --- Term helpers ---

    def term_buckets(self) -> list[int]:
        s = self.last()
        if s is None:
            return []
        snap_ts = _snap_ts(s)
        expiries = s.get_expiry_keys_ms()
        return sorted({  # unique term buckets only; guarantee no duplicates in output list  # +
            _term_key_ms(snap_ts=snap_ts, expiry_ts=ex, term_bucket_ms=self.term_bucket_ms)
            for ex in expiries
        })  # +

    def last_for_term(self, term_key_ms: int) -> OptionChainSnapshot | None:
        base = self.main.last()
        return (
            None
            if base is None
            else OptionChainSnapshotView.for_term_bucket(
                base=base,
                term_key_ms=term_key_ms,
                term_bucket_ms=self.term_bucket_ms,
            )
        )

    def get_at_or_before_for_term(self, term_key_ms: int, timestamp: int) -> OptionChainSnapshot | None:
        base = self.main.get_at_or_before(timestamp)
        return (
            None
            if base is None
            else OptionChainSnapshotView.for_term_bucket(
                base=base,
                term_key_ms=term_key_ms,
                term_bucket_ms=self.term_bucket_ms,
            )
        )

    def window_for_term(self, term_key_ms: int, n: int) -> Iterable[OptionChainSnapshot]:
        out: list[OptionChainSnapshot] = []
        for base in self.main.window(n):
            out.append(
                OptionChainSnapshotView.for_term_bucket(
                    base=base,
                    term_key_ms=term_key_ms,
                    term_bucket_ms=self.term_bucket_ms,
                )
            )
        return out

    def get_n_before_for_term(self, term_key_ms: int, timestamp: int, n: int) -> Iterable[OptionChainSnapshot]:
        out: list[OptionChainSnapshot] = []
        for base in self.main.get_n_before(timestamp, n):
            out.append(
                OptionChainSnapshotView.for_term_bucket(
                    base=base,
                    term_key_ms=term_key_ms,
                    term_bucket_ms=self.term_bucket_ms,
                )
            )
        return out

    def window_df_for_term(self, term_key_ms: int, n: int | None = None) -> pd.DataFrame:
        k = self.default_term_window if n is None else int(n)
        return _concat_frames([s.frame for s in self.window_for_term(term_key_ms, k)])

    def get_n_before_df_for_term(self, term_key_ms: int, timestamp: int, n: int | None = None) -> pd.DataFrame:
        k = self.default_term_window if n is None else int(n)
        return _concat_frames([s.frame for s in self.get_n_before_for_term(term_key_ms, timestamp, k)])

    # --- Expiry helpers ---

    def expiries(self) -> list[int]:
        s = self.last()
        if s is None:
            return []
        return sorted(s.get_expiry_keys_ms())

    def last_for_expiry(self, expiry_ts: int) -> OptionChainSnapshot | None:
        base = self.main.last()
        return None if base is None else OptionChainSnapshotView.for_expiry(base=base, expiry_ts=expiry_ts)

    def get_at_or_before_for_expiry(self, expiry_ts: int, timestamp: int) -> OptionChainSnapshot | None:
        base = self.main.get_at_or_before(timestamp)
        return None if base is None else OptionChainSnapshotView.for_expiry(base=base, expiry_ts=expiry_ts)

    def window_for_expiry(self, expiry_ts: int, n: int) -> Iterable[OptionChainSnapshot]:
        out: list[OptionChainSnapshot] = []
        for base in self.main.window(n):
            out.append(OptionChainSnapshotView.for_expiry(base=base, expiry_ts=expiry_ts))
        return out

    def get_n_before_for_expiry(self, expiry_ts: int, timestamp: int, n: int) -> Iterable[OptionChainSnapshot]:
        out: list[OptionChainSnapshot] = []
        for base in self.main.get_n_before(timestamp, n):
            out.append(OptionChainSnapshotView.for_expiry(base=base, expiry_ts=expiry_ts))
        return out

    def window_df_for_expiry(self, expiry_ts: int, n: int | None = None) -> pd.DataFrame:
        k = self.default_expiry_window if n is None else int(n)
        return _concat_frames([s.frame for s in self.window_for_expiry(expiry_ts, k)])

    def get_n_before_df_for_expiry(self, expiry_ts: int, timestamp: int, n: int | None = None) -> pd.DataFrame:
        k = self.default_expiry_window if n is None else int(n)
        return _concat_frames([s.frame for s in self.get_n_before_for_expiry(expiry_ts, timestamp, k)])


# Public aliases (constructors)
OptionChainSimpleCache = DequeSnapshotCache
OptionChainExpiryCache = OptionChainExpiryIndexedCache
OptionChainTermBucketedCache = OptionChainTermBucketedCache

# Type alias for handler annotations (interface type)
OptionChainCache: TypeAlias = SnapshotCache[OptionChainSnapshot]

# Default concrete cache (optional)
OptionChainDefaultCache = OptionChainExpiryIndexedCache
