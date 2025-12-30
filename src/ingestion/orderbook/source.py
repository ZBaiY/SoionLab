from __future__ import annotations

import time
import threading
from typing import AsyncIterable, Iterator, AsyncIterator, Iterable
from pathlib import Path
from ingestion.contracts.source import Source, AsyncSource, Raw
from ingestion.contracts.tick import _to_interval_ms, _guard_interval_ms

from quant_engine.utils.paths import data_root_from_file, resolve_under_root

DATA_ROOT = data_root_from_file(__file__, levels_up=3)

class OrderbookFileSource(Source):
    """
    Orderbook source backed by local parquet files.

    Layout:
        root/
          └── <symbol>/
              ├── snapshot_2025-01-01.parquet
              ├── snapshot_2025-01-02.parquet
              └── ...
    """

    def __init__(self, *, root: str | Path, symbol: str):
        self._root = resolve_under_root(DATA_ROOT, root, strip_prefix="data")
        self._symbol = symbol

        self._path = self._root / symbol
        if not self._path.exists():
            raise FileNotFoundError(f"Orderbook path does not exist: {self._path}")

    def __iter__(self) -> Iterator[Raw]:
        try:
            import pandas as pd
        except ImportError as e:
            raise RuntimeError("pandas is required for OrderbookFileSource parquet loading") from e

        files = sorted(self._path.glob("snapshot_*.parquet"))
        if not files:
            raise FileNotFoundError(f"No orderbook parquet files found under {self._path}")

        for fp in files:
            df = pd.read_parquet(fp)
            for _, row in df.iterrows():
                yield row.to_dict()


class OrderbookRESTSource(Source):
    """
    Orderbook source using REST-style polling.
    """

    def __init__(
        self,
        *,
        fetch_fn,
        backfill_fn=None,
        interval: str | None = None,
        interval_ms: int | None = None,
        poll_interval: float | None = None,
        stop_event: threading.Event | None = None,
    ):
        """
        Parameters
        ----------
        fetch_fn:
            external callable returning an iterable of raw orderbook payloads.
            Authentication, pagination, retries, and vendor-specific logic live inside fetch_fn.
        """
        self._fetch_fn = fetch_fn
        self._backfill_fn = backfill_fn
        self._interval = interval
        self._stop_event = stop_event
        if interval_ms is not None:
            self._interval_ms = interval_ms
        elif interval is not None:
            self._interval_ms = _to_interval_ms(interval)
            if self._interval_ms is None:
                raise ValueError(f"Invalid interval format: {interval!r}")
            _guard_interval_ms(interval, self._interval_ms)
        elif poll_interval is not None:
            self._interval_ms = int(round(poll_interval * 1000))
        else:
            raise ValueError("One of interval_ms, interval, or poll_interval must be provided")

    def __iter__(self) -> Iterator[Raw]:
        while True:
            if self._stop_event is not None and self._stop_event.is_set():
                return
            rows = self._fetch_fn()
            for row in rows:
                yield row
            assert self._interval_ms is not None
            if self._sleep_or_stop(self._interval_ms / 1000.0):
                return

    def _sleep_or_stop(self, seconds: float) -> bool:
        if self._stop_event is None:
            time.sleep(seconds)
            return False
        return self._stop_event.wait(seconds)

    def backfill(self, *, start_ts: int, end_ts: int) -> Iterable[Raw]:
        if self._backfill_fn is None:
            raise NotImplementedError("OrderbookRESTSource backfill requires backfill_fn")
        return self._backfill_fn(start_ts=int(start_ts), end_ts=int(end_ts))


class OrderbookWebSocketSource(AsyncSource):
    """
    Orderbook source backed by a WebSocket async iterator.
    """

    def __init__(self, stream: AsyncIterable[Raw] | None = None):
        self._stream = stream

    def __aiter__(self) -> AsyncIterator[Raw]:
        async def _gen():
            assert self._stream is not None, "stream must be provided for OrderbookWebSocketSource"
            async for msg in self._stream:
                yield msg

        return _gen()
