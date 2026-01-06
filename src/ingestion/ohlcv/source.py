from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any, Iterable, AsyncIterable, Iterator, AsyncIterator, Callable, Mapping
from pathlib import Path
import threading
import os

import requests
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.contracts.source import Source, AsyncSource, Raw
from ingestion.contracts.tick import _guard_interval_ms

from quant_engine.utils.paths import data_root_from_file, resolve_under_root
from quant_engine.utils.logger import get_logger, log_debug, log_exception

DATA_ROOT = data_root_from_file(__file__, levels_up=3)
_RAW_OHLCV_ROOT = DATA_ROOT / "raw" / "ohlcv"
_LOG = get_logger(__name__)
_LOCK_WARN_S = 0.2
_WRITE_LOG_EVERY = 100


class OHLCVWriteError(RuntimeError):
    """Raised for non-recoverable OHLCV parquet write failures."""

"""Raw payload keys expected (ingestion boundary):

OHLCV sources should yield dict-like rows that are *exchange-typed* but already
normalized to stable field names.

Minimum required:
    - open_time: epoch milliseconds int
    - close_time: epoch milliseconds int

Canonical:
    - data_ts: epoch milliseconds int (recommended) == close_time
"""

# --- Binance REST helpers ---

def _now_ms() -> int:
    return int(time.time() * 1000.0)


def _date_path(root: Path, *, symbol: str, interval: str, data_ts: int) -> Path:
    dt = datetime.fromtimestamp(int(data_ts) / 1000.0, tz=timezone.utc)
    year = dt.strftime("%Y")
    ymd = dt.strftime("%Y_%m_%d")
    return root / symbol / interval / year / f"{ymd}.parquet"


def _binance_klines_rest(
    *,
    symbol: str,
    interval: str,
    limit: int = 2,
    start_time: int | None = None,
    end_time: int | None = None,
    base_url: str = "https://api.binance.com",
    timeout: float = 10.0,
) -> list[dict[str, Any]]:
    """Fetch klines from Binance REST and return rows as dicts.

    Binance endpoint: GET /api/v3/klines
    Response is a list of lists:
        [
          [open_time, open, high, low, close, volume, close_time,
           quote_asset_volume, number_of_trades,
           taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore],
          ...
        ]

    Returned dicts use the same field names as BINANCE_KLINE_FIELDS.
    """
    url = base_url.rstrip("/") + "/api/v3/klines"
    params: dict[str, Any] = {
        "symbol": symbol,
        "interval": interval,
        "limit": int(limit),
    }
    if start_time is not None:
        params["startTime"] = int(start_time)
    if end_time is not None:
        params["endTime"] = int(end_time)

    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected Binance klines response type: {type(data)!r}")

    out: list[dict[str, Any]] = []
    for row in data:
        if not isinstance(row, list) or len(row) < 12:
            continue
        out.append(
            {
                "open_time": int(row[0]),
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                "volume": row[5],
                "close_time": int(row[6]),
                "quote_asset_volume": row[7],
                "number_of_trades": int(row[8]),
                "taker_buy_base_asset_volume": row[9],
                "taker_buy_quote_asset_volume": row[10],
                "ignore": row[11],
                # canonical anti-lookahead timestamp: CLOSE time
                "data_ts": int(row[6]),
            }
        )
    return out


def make_binance_kline_fetch_fn(
    *,
    symbol: str,
    interval: str,
    limit: int = 2,
    base_url: str = "https://api.binance.com",
    timeout: float = 10.0,
) -> Callable[[], Iterable[Raw]]:
    """Factory returning a fetch_fn compatible with OHLCVRESTSource.

    This fetch_fn returns the latest `limit` klines and includes `data_ts == close_time`.
    """

    def _fetch() -> Iterable[Raw]:
        return _binance_klines_rest(
            symbol=symbol,
            interval=interval,
            limit=limit,
            base_url=base_url,
            timeout=timeout,
        )

    return _fetch


class BinanceKlinesRESTSource(Source):
    """Binance klines source via REST polling.

    Emits only newly observed *closed* bars (canonical data_ts == close_time).

    Notes:
      - Binance klines REST typically includes the currently-forming bar.
        We request `limit=2` and emit the penultimate bar (the last fully closed bar).
      - Output rows include `data_ts == close_time` for anti-lookahead.
    """

    def __init__(
        self,
        *,
        symbol: str,
        interval: str,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        base_url: str = "https://api.binance.com",
        timeout: float = 10.0,
        root: str | Path = _RAW_OHLCV_ROOT,
        stop_event: threading.Event | None = None,
    ):
        self._symbol = symbol
        self._interval = interval
        self._base_url = base_url
        self._timeout = float(timeout)
        self._root = resolve_under_root(DATA_ROOT, root, strip_prefix="data")
        self._root.mkdir(parents=True, exist_ok=True)
        self._stop_event = stop_event
        self._used_paths: set[Path] = set()

        if poll_interval_ms is not None:
            self._poll_interval_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            # sensible default: 1000ms
            self._poll_interval_ms = 1000

        _guard_interval_ms(self._interval, self._poll_interval_ms)
        if self._poll_interval_ms <= 0:
            raise ValueError(f"poll interval must be > 0ms, got {self._poll_interval_ms}")

        self._last_close_time: int | None = None
        self._write_count = 0

    _global_lock = threading.Lock()
    _global_writers: dict[Path, pq.ParquetWriter] = {}
    _global_schemas: dict[Path, pa.Schema] = {}
    _global_locks: dict[Path, threading.Lock] = {}
    _global_refs: dict[Path, int] = {}

    def __iter__(self) -> Iterator[Raw]:
        try:
            while True:
                if self._stop_event is not None and self._stop_event.is_set():
                    return
                try:
                    rows = _binance_klines_rest(
                        symbol=self._symbol,
                        interval=self._interval,
                        limit=2,
                        base_url=self._base_url,
                        timeout=self._timeout,
                    )
                except Exception:
                    # fail-soft: transient network errors
                    if self._sleep_or_stop(self._poll_interval_ms / 1000.0):
                        return
                    continue

                if len(rows) >= 2:
                    # penultimate is the last fully closed bar
                    bar = rows[-2]
                elif len(rows) == 1:
                    # fallback: if API returns only 1 bar, treat it as closed if its close_time < now
                    bar = rows[0]
                    try:
                        if int(bar.get("close_time", 0)) >= _now_ms():
                            bar = {}
                    except Exception:
                        bar = {}
                else:
                    bar = {}

                if bar:
                    ct = int(bar["close_time"])
                    if self._last_close_time is None or ct > self._last_close_time:
                        self._last_close_time = ct
                        self._write_raw_snapshot(bar)
                        yield bar

                if self._sleep_or_stop(self._poll_interval_ms / 1000.0):
                    return
        finally:
            self.close()

    def _sleep_or_stop(self, seconds: float) -> bool:
        if self._stop_event is None:
            time.sleep(seconds)
            return False
        return self._stop_event.wait(seconds)

    def backfill(self, *, start_ts: int, end_ts: int, limit: int = 1000) -> Iterable[Raw]:
        return _binance_klines_rest(
            symbol=self._symbol,
            interval=self._interval,
            limit=int(limit),
            start_time=int(start_ts),
            end_time=int(end_ts),
            base_url=self._base_url,
            timeout=self._timeout,
        )

    def _write_raw_snapshot(self, bar: Mapping[str, Any]) -> None:
        data_ts = int(bar.get("data_ts", bar.get("close_time", 0)))
        if data_ts <= 0:
            raise OHLCVWriteError("Missing or invalid data_ts for OHLCV write")
        path = _date_path(self._root, symbol=self._symbol, interval=self._interval, data_ts=data_ts)
        path.parent.mkdir(parents=True, exist_ok=True)
        row = dict(bar)
        row["data_ts"] = int(row.get("data_ts", data_ts))
        row["open_time"] = int(row.get("open_time", 0))
        row["close_time"] = int(row.get("close_time", 0))
        lock = self._get_lock(path)
        write_start = time.monotonic()
        start = time.monotonic()
        lock.acquire()
        waited_s = time.monotonic() - start
        if waited_s > _LOCK_WARN_S:
            log_debug(
                _LOG,
                "ingestion.lock_wait",
                component="ohlcv",
                path=str(path),
                wait_ms=int(waited_s * 1000.0),
            )
        try:
            writer, schema, bak_path = self._get_writer_and_schema(path, row)
            aligned = self._align_row_to_schema(row, schema, path)
            table = pa.Table.from_pydict(aligned, schema=schema)
            writer.write_table(table)
            if bak_path is not None:
                os.remove(bak_path)
            self._write_count += 1
            if self._write_count % _WRITE_LOG_EVERY == 0:
                log_debug(
                    _LOG,
                    "ingestion.write_sample",
                    component="ohlcv",
                    path=str(path),
                    rows=int(table.num_rows),
                    write_ms=int((time.monotonic() - write_start) * 1000),
                    write_seq=self._write_count,
                )
        except Exception as exc:
            raise OHLCVWriteError(str(exc)) from exc
        finally:
            lock.release()

    def _get_lock(self, path: Path) -> threading.Lock:
        with self._global_lock:
            lock = self._global_locks.get(path)
            if lock is None:
                lock = threading.Lock()
                self._global_locks[path] = lock
            return lock

    def _get_writer_and_schema(
        self,
        path: Path,
        row: Mapping[str, Any],
    ) -> tuple[pq.ParquetWriter, pa.Schema, Path | None]:
        with self._global_lock:
            writer = self._global_writers.get(path)
            if writer is not None:
                if path not in self._used_paths:
                    self._global_refs[path] = self._global_refs.get(path, 0) + 1
                    self._used_paths.add(path)
                return writer, self._global_schemas[path], None

        if path.exists():
            return self._bootstrap_existing(path)

        table = pa.Table.from_pydict(self._align_row_to_schema(row, None, path))
        writer = pq.ParquetWriter(path, table.schema)
        with self._global_lock:
            self._global_writers[path] = writer
            self._global_schemas[path] = table.schema
            self._global_refs[path] = self._global_refs.get(path, 0) + 1
        self._used_paths.add(path)
        return writer, table.schema, None

    def _bootstrap_existing(self, path: Path) -> tuple[pq.ParquetWriter, pa.Schema, Path]:
        bak_path = path.with_suffix(".parquet.bak")
        os.replace(path, bak_path)
        try:
            table = pq.read_table(bak_path)
            schema = table.schema
            writer = pq.ParquetWriter(path, schema)
            writer.write_table(table)
            with self._global_lock:
                self._global_writers[path] = writer
                self._global_schemas[path] = schema
                self._global_refs[path] = self._global_refs.get(path, 0) + 1
            self._used_paths.add(path)
            return writer, schema, bak_path
        except Exception as exc:
            raise OHLCVWriteError(
                f"Failed to bootstrap parquet writer for {path}; "
                f"backup retained at {bak_path}: {exc}"
            )

    def _align_row_to_schema(
        self,
        row: Mapping[str, Any],
        schema: pa.Schema | None,
        path: Path,
    ) -> dict[str, list[Any]]:
        cols = list(schema.names) if schema is not None else list(row.keys())
        extra = [c for c in row.keys() if c not in cols]
        if extra:
            raise OHLCVWriteError(f"OHLCV schema drift for {path}: unexpected columns {extra}")
        out: dict[str, list[Any]] = {}
        for c in cols:
            out[c] = [row.get(c, None)]
        return out

    def close(self) -> None:
        with self._global_lock:
            to_close: list[tuple[Path, pq.ParquetWriter]] = []
            for path in list(self._used_paths):
                ref = self._global_refs.get(path, 0) - 1
                if ref <= 0:
                    writer = self._global_writers.pop(path, None)
                    if writer is not None:
                        to_close.append((path, writer))
                    self._global_refs.pop(path, None)
                    self._global_schemas.pop(path, None)
                    self._global_locks.pop(path, None)
                else:
                    self._global_refs[path] = ref
            self._used_paths.clear()
        for _, writer in to_close:
            try:
                writer.close()
            except Exception as exc:
                log_exception(
                    _LOG,
                    "ingestion.writer_close_error",
                    component="ohlcv",
                    err_type=type(exc).__name__,
                    err=str(exc),
                )


class OHLCVFileSource(Source):
    """
    OHLCV source backed by local parquet files.

    Layout:
        root/
          └── <symbol>/
              └── <interval>/
                  ├── 2023.parquet
                  ├── 2024.parquet
                  └── ...
    """

    def __init__(
        self,
        *,
        root: str | Path,
        start_ts: int | None = None,
        end_ts: int | None = None,
        paths: Iterable[Path] | None = None,
        **kwargs,
    ):
        self._root = resolve_under_root(DATA_ROOT, root, strip_prefix="data")
        self._symbol = kwargs.get("symbol")
        self._interval = kwargs.get("interval")
        assert self._symbol is not None and isinstance(self._symbol, str), "symbol must be provided as a string"
        assert self._interval is not None and isinstance(self._interval, str), "interval must be provided as a string"
        self.interval_ms = self._interval_milliseconds(self._interval)
        self._interval_ms = self.interval_ms  # backward-compatible alias
        self._path = self._root / self._symbol / self._interval
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
        self._start_ts = int(start_ts) if start_ts is not None else None
        self._end_ts = int(end_ts) if end_ts is not None else None
        if self._paths is None and not self._path.exists():
            raise FileNotFoundError(f"OHLCV path does not exist: {self._path}")

    @staticmethod
    def _interval_milliseconds(interval: str) -> int:
        # supports: 1m/15m/1h/4h/1d/1w
        n = int(interval[:-1])
        u = interval[-1].lower()
        if u == "m":
            return n * 60_000
        if u == "h":
            return n * 3_600_000
        if u == "d":
            return n * 86_400_000
        if u == "w":
            return n * 7 * 86_400_000
        raise ValueError(f"Unsupported interval: {interval!r}")

    @staticmethod
    def _coerce_ts(x: Any) -> int:
        """Return unix epoch milliseconds as int. Accepts seconds/ms, datetime, pandas Timestamp, str."""
        if x is None:
            raise ValueError("timestamp is None")

        if isinstance(x, bool):
            raise ValueError("invalid timestamp type: bool")

        if isinstance(x, (int, float)):
            v = float(x)
            # heuristic: seconds ~1e9, ms ~1e12
            if v < 10_000_000_000:
                return int(round(v * 1000.0))
            return int(round(v))

        if isinstance(x, datetime):
            dt = x if x.tzinfo else x.replace(tzinfo=timezone.utc)
            return int(round(dt.timestamp() * 1000.0))

        # pandas.Timestamp / numpy datetime64 / ISO str / etc.
        import pandas as pd  # local import to keep module optional

        ts = pd.to_datetime(x, utc=True, errors="raise")
        if hasattr(ts, "to_pydatetime"):
            return int(round(ts.to_pydatetime().timestamp() * 1000.0))
        return int(round(pd.Timestamp(ts).to_pydatetime().timestamp() * 1000.0))

    def __iter__(self) -> Iterator[Raw]:
        try:
            import pandas as pd
        except ImportError as e:
            raise RuntimeError("pandas is required for OHLCVFileSource parquet loading") from e

        if self._paths is not None:
            files = [p for p in self._paths if p.exists()]
        elif self._start_ts is not None or self._end_ts is not None:
            start_date = datetime.fromtimestamp((self._start_ts or 0) / 1000.0, tz=timezone.utc).date()
            end_date = datetime.fromtimestamp((self._end_ts or int(time.time() * 1000)) / 1000.0, tz=timezone.utc).date()
            start_year = start_date.year
            end_year = end_date.year
            year_dirs = [p for p in self._path.glob("[0-9][0-9][0-9][0-9]") if p.is_dir()]
            files: list[Path] = []
            if year_dirs:
                for year in range(int(start_year), int(end_year) + 1):
                    yd = self._path / f"{year}"
                    if not yd.is_dir():
                        continue
                    for fp in yd.glob("*.parquet"):
                        stem = fp.stem
                        parts = stem.split("_")
                        if len(parts) != 3 or not all(p.isdigit() for p in parts):
                            continue
                        try:
                            d = datetime(int(parts[0]), int(parts[1]), int(parts[2]), tzinfo=timezone.utc).date()
                        except Exception:
                            continue
                        if start_date <= d <= end_date:
                            files.append(fp)
                files.sort(key=lambda p: p.stem)
            if not files:
                files = [
                    self._path / f"{year}.parquet"
                    for year in range(int(start_year), int(end_year) + 1)
                    if (self._path / f"{year}.parquet").exists()
                ]
        else:
            files = sorted(self._path.glob("*.parquet"))
        if not files:
            return

        dt = int(self.interval_ms)

        for fp in files:
            df = pd.read_parquet(fp)

            # ---- canonical timestamp: close-time to avoid lookahead ----
            if "data_ts" not in df.columns:
                if "close_time" in df.columns:
                    df["data_ts"] = df["close_time"].map(self._coerce_ts)
                elif "closed_time" in df.columns:
                    df["data_ts"] = df["closed_time"].map(self._coerce_ts)
                elif "timestamp" in df.columns:
                    # legacy alias
                    df["data_ts"] = df["timestamp"].map(self._coerce_ts)
                elif "open_time" in df.columns:
                    # data available at bar close => open_time + interval
                    df["data_ts"] = df["open_time"].map(self._coerce_ts) + int(dt)
                else:
                    raise ValueError(
                        f"Parquet file {fp} missing 'data_ts' and no fallback "
                        f"('close_time'/'closed_time'/'timestamp'/'open_time') columns found"
                    )
            else:
                df["data_ts"] = df["data_ts"].map(self._coerce_ts)

            if self._start_ts is not None:
                df = df[df["data_ts"] >= int(self._start_ts)]
            if self._end_ts is not None:
                df = df[df["data_ts"] <= int(self._end_ts)]
            if df.empty:
                continue
            df = df.sort_values("data_ts", kind="mergesort")
            df["data_ts"] = df["data_ts"].astype("int64", copy=False)

            # Ensure canonical event-time field name on output
            if "timestamp" in df.columns:
                df = df.drop(columns=["timestamp"])

            # pandas types: dict[Hashable, Any] -> dict[str, Any]
            for rec in df.to_dict(orient="records"):
                out_rec = {str(k): v for k, v in rec.items()}
                # extra safety: if a legacy timestamp sneaks in, normalize it
                if "timestamp" in out_rec and "data_ts" not in out_rec:
                    out_rec["data_ts"] = self._coerce_ts(out_rec.pop("timestamp"))
                elif "timestamp" in out_rec:
                    out_rec.pop("timestamp", None)
                yield out_rec

class OHLCVRESTSource(Source):
    """
    OHLCV source using REST-style polling.
    """

    def __init__(
        self,
        *,
        fetch_fn: Callable[[], Iterable[Raw]],
        backfill_fn: Callable[..., Iterable[Raw]] | None = None,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        stop_event: threading.Event | None = None,
    ):
        """
        Parameters
        ----------
        fetch_fn:
            Callable returning an iterable of raw OHLCV payloads.
            The function itself handles authentication / pagination.
        poll_interval:
            Backward-compatible polling cadence in seconds.
        poll_interval_ms:
            Polling cadence in epoch milliseconds.
        interval:
            Polling cadence as an interval string (e.g. "250ms", "1s", "1m").
            If provided, it takes precedence over poll_interval.
        """
        self._fetch_fn = fetch_fn
        self._backfill_fn = backfill_fn
        self._stop_event = stop_event

        if poll_interval_ms is not None:
            self._poll_interval_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            raise ValueError("One of poll_interval, poll_interval_ms, or interval must be provided")

        if self._poll_interval_ms <= 0:
            raise ValueError(f"poll interval must be > 0ms, got {self._poll_interval_ms}")

    def __iter__(self) -> Iterator[Raw]:
        while True:
            if self._stop_event is not None and self._stop_event.is_set():
                return
            rows = self._fetch_fn()
            for row in rows:
                yield row
            if self._sleep_or_stop(self._poll_interval_ms / 1000.0):
                return

    def _sleep_or_stop(self, seconds: float) -> bool:
        if self._stop_event is None:
            time.sleep(seconds)
            return False
        return self._stop_event.wait(seconds)

    def backfill(self, *, start_ts: int, end_ts: int) -> Iterable[Raw]:
        if self._backfill_fn is None:
            raise NotImplementedError("OHLCVRESTSource backfill requires backfill_fn")
        return self._backfill_fn(start_ts=int(start_ts), end_ts=int(end_ts))


class OHLCVWebSocketSource(AsyncSource):
    """
    OHLCV source backed by a WebSocket async iterator.

    Intended for:
        - realtime streaming klines
    """

    def __init__(self, stream: AsyncIterable[Raw] | None = None):
        self._stream = stream

    def __aiter__(self) -> AsyncIterator[Raw]:
        async def _gen():
            assert self._stream is not None, "stream must be provided for OHLCVWebSocketSource"
            async for msg in self._stream:
                yield msg

        return _gen()
