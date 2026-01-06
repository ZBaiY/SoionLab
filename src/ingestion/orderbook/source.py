from __future__ import annotations

import time
import threading
from datetime import datetime, timezone
from typing import AsyncIterable, Iterator, AsyncIterator, Iterable, Callable
from pathlib import Path
from ingestion.contracts.source import Source, AsyncSource, Raw
from ingestion.contracts.tick import _to_interval_ms, _guard_interval_ms, _coerce_epoch_ms

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

    def __init__(
        self,
        *,
        root: str | Path,
        symbol: str,
        start_ts: int | None = None,
        end_ts: int | None = None,
        paths: Iterable[Path] | None = None,
    ):
        self._root = resolve_under_root(DATA_ROOT, root, strip_prefix="data")
        self._symbol = symbol
        self._start_ts = int(start_ts) if start_ts is not None else None
        self._end_ts = int(end_ts) if end_ts is not None else None
        if paths is not None:
            resolved_paths: list[Path] = []
            for p in paths:
                path = Path(p)
                if not path.is_absolute():
                    path = self._root / path
                resolved_paths.append(path)
            self._paths: list[Path] | None = resolved_paths
        else:
            self._paths = None

        self._path = self._root / symbol

    def __iter__(self) -> Iterator[Raw]:
        try:
            import pandas as pd
        except ImportError as e:
            raise RuntimeError("pandas is required for OrderbookFileSource parquet loading") from e

        if self._paths is not None:
            files = [p for p in self._paths if p.exists()]
        elif self._path.exists():
            files = sorted(self._path.glob("snapshot_*.parquet"))
        else:
            files = []
        if self._start_ts is not None or self._end_ts is not None:
            start_date = datetime.fromtimestamp((self._start_ts or 0) / 1000.0, tz=timezone.utc).date()
            end_date = datetime.fromtimestamp((self._end_ts or int(time.time() * 1000)) / 1000.0, tz=timezone.utc).date()
            pruned: list[Path] = []
            for fp in files:
                stem = fp.stem
                if not stem.startswith("snapshot_"):
                    continue
                try:
                    d = datetime.strptime(stem.replace("snapshot_", ""), "%Y-%m-%d").date()
                except Exception:
                    continue
                if start_date <= d <= end_date:
                    pruned.append(fp)
            files = pruned
        if not files:
            return

        for fp in files:
            df = pd.read_parquet(fp)
            if self._start_ts is not None or self._end_ts is not None:
                ts_col = None
                if "data_ts" in df.columns:
                    ts_col = "data_ts"
                elif "ts" in df.columns:
                    ts_col = "ts"
                elif "timestamp" in df.columns:
                    ts_col = "timestamp"
                if ts_col is not None:
                    ts = df[ts_col].map(_coerce_epoch_ms)
                    if self._start_ts is not None:
                        df = df[ts >= int(self._start_ts)]
                    if self._end_ts is not None:
                        df = df[ts <= int(self._end_ts)]
                    if df.empty:
                        continue
            for _, row in df.iterrows():
                yield row.to_dict()


class OrderbookRESTSource(Source):
    """
    Orderbook source using REST-style polling.
    """

    def __init__(
        self,
        *,
        fetch_fn: Callable[[], Iterable[Raw]],
        backfill_fn: Callable[..., Iterable[Raw]] | None = None,
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
