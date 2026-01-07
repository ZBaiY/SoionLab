from __future__ import annotations

import time
import threading
from datetime import datetime, timezone
from typing import AsyncIterable, Iterator, AsyncIterator, Iterable, Callable, Mapping, Any
from pathlib import Path
import os
import pyarrow as pa
import pyarrow.parquet as pq
from ingestion.contracts.source import Source, AsyncSource, Raw
from ingestion.contracts.tick import _to_interval_ms, _guard_interval_ms, _coerce_epoch_ms

from quant_engine.utils.paths import data_root_from_file, resolve_under_root
from quant_engine.utils.logger import get_logger, log_debug

DATA_ROOT = data_root_from_file(__file__, levels_up=3)
_LOG = get_logger(__name__)
_LOCK_WARN_S = 0.2
_WRITE_LOG_EVERY = 100

_GLOBAL_LOCK = threading.Lock()
_GLOBAL_WRITERS: dict[Path, pq.ParquetWriter] = {}
_GLOBAL_SCHEMAS: dict[Path, pa.Schema] = {}
_GLOBAL_LOCKS: dict[Path, threading.Lock] = {}
_GLOBAL_REFS: dict[Path, int] = {}


def _snapshot_path(root: Path, *, symbol: str, data_ts: int) -> Path:
    dt = datetime.fromtimestamp(int(data_ts) / 1000.0, tz=timezone.utc)
    ymd = dt.strftime("%Y-%m-%d")
    return root / symbol / f"snapshot_{ymd}.parquet"


def _get_lock(path: Path) -> threading.Lock:
    with _GLOBAL_LOCK:
        lock = _GLOBAL_LOCKS.get(path)
        if lock is None:
            lock = threading.Lock()
            _GLOBAL_LOCKS[path] = lock
        return lock


def _align_row_to_schema(
    row: Mapping[str, Any],
    schema: pa.Schema | None,
    path: Path,
) -> dict[str, list[Any]]:
    cols = list(schema.names) if schema is not None else list(row.keys())
    extra = [c for c in row.keys() if c not in cols]
    if extra:
        raise ValueError(f"Orderbook schema drift for {path}: unexpected columns {extra}")
    out: dict[str, list[Any]] = {}
    for c in cols:
        out[c] = [row.get(c, None)]
    return out


def _bootstrap_existing(path: Path) -> tuple[pq.ParquetWriter, pa.Schema, Path]:
    bak_path = path.with_suffix(".parquet.bak")
    os.replace(path, bak_path)
    try:
        table = pq.read_table(bak_path)
        schema = table.schema
        writer = pq.ParquetWriter(path, schema)
        writer.write_table(table)
        with _GLOBAL_LOCK:
            _GLOBAL_WRITERS[path] = writer
            _GLOBAL_SCHEMAS[path] = schema
            _GLOBAL_REFS[path] = _GLOBAL_REFS.get(path, 0) + 1
        return writer, schema, bak_path
    except Exception as exc:
        raise RuntimeError(
            f"Failed to bootstrap parquet writer for {path}; "
            f"backup retained at {bak_path}: {exc}"
        )


def _get_writer_and_schema(
    path: Path,
    row: Mapping[str, Any],
    used_paths: set[Path],
) -> tuple[pq.ParquetWriter, pa.Schema, Path | None]:
    with _GLOBAL_LOCK:
        writer = _GLOBAL_WRITERS.get(path)
        if writer is not None:
            if path not in used_paths:
                _GLOBAL_REFS[path] = _GLOBAL_REFS.get(path, 0) + 1
                used_paths.add(path)
            return writer, _GLOBAL_SCHEMAS[path], None

    if path.exists():
        writer, schema, bak_path = _bootstrap_existing(path)
        used_paths.add(path)
        return writer, schema, bak_path

    table = pa.Table.from_pydict(_align_row_to_schema(row, None, path))
    writer = pq.ParquetWriter(path, table.schema)
    with _GLOBAL_LOCK:
        _GLOBAL_WRITERS[path] = writer
        _GLOBAL_SCHEMAS[path] = table.schema
        _GLOBAL_REFS[path] = _GLOBAL_REFS.get(path, 0) + 1
    used_paths.add(path)
    return writer, table.schema, None


def _write_raw_snapshot(
    *,
    root: Path,
    symbol: str,
    row: Mapping[str, Any],
    used_paths: set[Path],
    write_counter: list[int] | None = None,
) -> None:
    ts_any = row.get("data_ts") or row.get("ts") or row.get("timestamp") or row.get("T") or row.get("E")
    data_ts = _coerce_epoch_ms(ts_any)
    path = _snapshot_path(root, symbol=symbol, data_ts=int(data_ts))
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = _get_lock(path)
    write_start = time.monotonic()
    start = time.monotonic()
    lock.acquire()
    waited_s = time.monotonic() - start
    if waited_s > _LOCK_WARN_S:
        log_debug(
            _LOG,
            "ingestion.lock_wait",
            component="orderbook",
            path=str(path),
            wait_ms=int(waited_s * 1000.0),
        )
    try:
        writer, schema, bak_path = _get_writer_and_schema(path, row, used_paths)
        aligned = _align_row_to_schema(row, schema, path)
        table = pa.Table.from_pydict(aligned, schema=schema)
        writer.write_table(table)
        if bak_path is not None:
            os.remove(bak_path)
        if write_counter is not None:
            write_counter[0] += 1
            write_count = write_counter[0]
        else:
            write_count = None
        if write_count is not None and write_count % _WRITE_LOG_EVERY == 0:
            log_debug(
                _LOG,
                "ingestion.write_sample",
                component="orderbook",
                path=str(path),
                rows=int(table.num_rows),
                write_ms=int((time.monotonic() - write_start) * 1000),
                write_seq=write_count,
            )
    finally:
        lock.release()

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
