from __future__ import annotations

import json
import time
import re
import datetime as dt
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Iterator
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ingestion.contracts.source import AsyncSource, Raw, Source
from ingestion.contracts.tick import _to_interval_ms, _guard_interval_ms, _coerce_epoch_ms

from quant_engine.utils.paths import data_root_from_file, resolve_under_root
from quant_engine.utils.logger import get_logger, log_exception

DATA_ROOT = data_root_from_file(__file__, levels_up=3)
_LOG = get_logger(__name__)

_GLOBAL_LOCK = threading.Lock()
_GLOBAL_LOCKS: dict[Path, threading.Lock] = {}


def _sentiment_path(root: Path, *, provider: str, data_ts: int) -> Path:
    dt_utc = dt.datetime.fromtimestamp(int(data_ts) / 1000.0, tz=dt.timezone.utc)
    return root / provider / f"{dt_utc.year:04d}" / f"{dt_utc.month:02d}" / f"{dt_utc.day:02d}.jsonl"


def _get_lock(path: Path) -> threading.Lock:
    with _GLOBAL_LOCK:
        lock = _GLOBAL_LOCKS.get(path)
        if lock is None:
            lock = threading.Lock()
            _GLOBAL_LOCKS[path] = lock
        return lock


def _write_raw_snapshot(*, root: Path, provider: str, row: Mapping[str, Any]) -> None:
    ts_any = row.get("timestamp") or row.get("published_at") or row.get("ts")
    data_ts = _coerce_epoch_ms(ts_any)
    path = _sentiment_path(root, provider=str(provider), data_ts=int(data_ts))
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = _get_lock(path)
    lock.acquire()
    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(dict(row), ensure_ascii=True) + "\n")
    except Exception as exc:
        log_exception(
            _LOG,
            "sentiment.persist_error",
            provider=str(provider),
            err_type=type(exc).__name__,
            err=str(exc),
        )
        raise
    finally:
        lock.release()


# -----------------------------------------------------------------------------
# Sentiment Sources
#
# Raw payload shape (what ingestion returns BEFORE normalization):
#   - timestamp: epoch ms int OR a datetime-like string (normalizer will coerce)
#   - text: str
#   - source: str (publisher/vendor), optional
#
# Example:
#   {"timestamp": 1764698847000, "text": "...", "source": "decrypt"}
#
# Notes:
#   - We do NOT store any sentiment score at ingestion; scoring is a downstream feature/model step.
#   - Source classes are IO-only; they MUST NOT mutate/enrich/normalize.
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class SentimentFileLayout:
    """Filesystem layout for sentiment JSONL."""

    # Root points to the sentiment folder, e.g. data/sentiment
    root: Path

    # Provider folder under root, e.g. "news" or "twitter".
    provider: str

    # File pattern under provider. Supports:
    #   - YYYY-MM-DD.jsonl
    #   - YYYY/MM/DD.jsonl
    #   - YYYY/MM/YYYY-MM-DD.jsonl
    #   - YYYY/MM/DD/<anything>.jsonl  (optional extra nesting)
    pattern: str = "**/*.jsonl"


_DATE_RE_YYYY_MM_DD = re.compile(r"^(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})$")
_DATE_RE_YYYYMMDD = re.compile(r"^(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})$")
_DATE_RE_DD = re.compile(r"^(?P<d>\d{2})$")


def _infer_ymd_from_path(fp: Path) -> tuple[int, int, int] | None:
    """Infer (year, month, day) from file path.

    Supported:
      - .../YYYY-MM-DD.jsonl
      - .../YYYY/MM/YYYY-MM-DD.jsonl
      - .../YYYY/MM/DD.jsonl
      - .../YYYY/MM/DD/<name>.jsonl  (DD inferred from parent folder)
      - .../YYYY/MM/DD.jsonl where stem is "01" and YYYY/MM from parents
    """
    stem = fp.stem

    m = _DATE_RE_YYYY_MM_DD.match(stem)
    if m:
        return int(m.group("y")), int(m.group("m")), int(m.group("d"))

    m = _DATE_RE_YYYYMMDD.match(stem)
    if m:
        return int(m.group("y")), int(m.group("m")), int(m.group("d"))

    # If filename is DD (e.g. 01.jsonl), use parents as YYYY/MM
    m = _DATE_RE_DD.match(stem)
    if m:
        try:
            d = int(m.group("d"))
            mm = int(fp.parent.name)
            yy = int(fp.parent.parent.name)
            # Validate date quickly
            dt.date(yy, mm, d)
            return yy, mm, d
        except Exception:
            return None

    # If nested under YYYY/MM/DD/<name>.jsonl, infer from parent folders
    try:
        d = int(fp.parent.name)
        mm = int(fp.parent.parent.name)
        yy = int(fp.parent.parent.parent.name)
        dt.date(yy, mm, d)
        return yy, mm, d
    except Exception:
        return None


def _file_sort_key(fp: Path) -> tuple[int, int, int, str]:
    ymd = _infer_ymd_from_path(fp)
    if ymd is None:
        # push unknowns to the end but keep deterministic ordering
        return (9999, 12, 31, str(fp))
    y, m, d = ymd
    return (y, m, d, str(fp))


class SentimentFileSource(Source):
    """Sentiment source backed by local JSONL files.

    Preferred:
        <root>/sentiment/<provider>/YYYY/MM/DD.jsonl  (e.g. 2025/12/01.jsonl)

    Also allowed:
        flat YYYY-MM-DD.jsonl and YYYY/MM/YYYY-MM-DD.jsonl

    Note:
        DD.jsonl is allowed when nested under YYYY/MM/.
    """

    def __init__(
        self,
        *,
        root: str | Path,
        provider: str,
        pattern: str = "**/*.jsonl",
        strict: bool = True,
        start_ts: int | None = None,
        end_ts: int | None = None,
        paths: Iterable[Path] | None = None,
    ) -> None:
        self._layout = SentimentFileLayout(
            root=resolve_under_root(DATA_ROOT, root, strip_prefix="data"),
            provider=str(provider),
            pattern=str(pattern),
        )
        self._strict = bool(strict)
        self._start_ts = int(start_ts) if start_ts is not None else None
        self._end_ts = int(end_ts) if end_ts is not None else None
        if paths is not None:
            resolved_paths: list[Path] = []
            for p in paths:
                path = Path(p)
                if not path.is_absolute():
                    path = self._layout.root / path
                resolved_paths.append(path)
            self._paths: list[Path] | None = resolved_paths
        else:
            self._paths = None

        self._path = self._layout.root / self._layout.provider
        if self._paths is None and self._strict and not self._path.exists():
            raise FileNotFoundError(f"Sentiment path does not exist: {self._path}")

    def __iter__(self) -> Iterator[Raw]:
        if self._paths is not None:
            files = [fp for fp in self._paths if fp.exists()]
        elif self._path.exists():
            files = sorted((fp for fp in self._path.glob(self._layout.pattern) if fp.is_file()), key=_file_sort_key)
        else:
            files = []
        if self._start_ts is not None or self._end_ts is not None:
            start_date = dt.datetime.fromtimestamp((self._start_ts or 0) / 1000.0, tz=dt.timezone.utc).date()
            end_date = dt.datetime.fromtimestamp((self._end_ts or int(time.time() * 1000)) / 1000.0, tz=dt.timezone.utc).date()
            pruned: list[Path] = []
            for fp in files:
                ymd = _infer_ymd_from_path(fp)
                if ymd is None:
                    continue
                d = dt.date(ymd[0], ymd[1], ymd[2])
                if start_date <= d <= end_date:
                    pruned.append(fp)
            files = pruned
        if self._paths is None and self._strict and not files:
            raise FileNotFoundError(f"No sentiment jsonl files found under {self._path}")

        for fp in files:
            with fp.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    # Do not normalize; just parse JSON.
                    try:
                        rec = json.loads(line)
                        if self._start_ts is not None or self._end_ts is not None:
                            ts_any = rec.get("timestamp") or rec.get("published_at") or rec.get("ts")
                            if ts_any is None:
                                continue
                            ts = _coerce_epoch_ms(ts_any)
                            if self._start_ts is not None and ts < int(self._start_ts):
                                continue
                            if self._end_ts is not None and ts > int(self._end_ts):
                                continue
                        yield rec
                    except Exception:
                        if self._strict:
                            raise
                        else:
                            continue


class SentimentRESTSource(Source):
    """Sentiment source using REST-style polling.

    `fetch_fn` is responsible for authentication, pagination, retries, and vendor logic.

    Interval semantics:
      - Prefer `interval` (e.g. "30s", "5m") or `interval_ms`.
      - If neither is provided, the source is invalid (would hot-loop).

    This Source yields Raw records returned by `fetch_fn`.
    """

    def __init__(
        self,
        *,
        fetch_fn: Callable[[], Iterable[Raw]],
        interval: str | None = None,
        interval_ms: int | None = None,
        poll_interval: float | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        self._fetch_fn = fetch_fn
        self._stop_event = stop_event

        if interval_ms is not None:
            self._interval_ms = int(interval_ms)
        elif interval is not None:
            ms = _to_interval_ms(interval)
            if ms is None:
                raise ValueError(f"Invalid interval format: {interval!r}")
            self._interval_ms = int(ms)
            _guard_interval_ms(interval, self._interval_ms)
        elif poll_interval is not None:
            # legacy compatibility
            self._interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            raise ValueError("One of interval_ms, interval, or poll_interval must be provided")

        if self._interval_ms <= 0:
            raise ValueError("interval_ms must be > 0")

    def __iter__(self) -> Iterator[Raw]:
        while True:
            if self._stop_event is not None and self._stop_event.is_set():
                return
            rows = self._fetch_fn()
            for row in rows:
                yield row
            if self._sleep_or_stop(self._interval_ms / 1000.0):
                return

    def _sleep_or_stop(self, seconds: float) -> bool:
        if self._stop_event is None:
            time.sleep(seconds)
            return False
        return self._stop_event.wait(seconds)


class SentimentStreamSource(AsyncSource):
    """Sentiment source backed by an async stream (e.g., WebSocket, queue, etc.)."""

    def __init__(self, *, stream: AsyncIterable[Raw] | None = None) -> None:
        self._stream = stream

    def __aiter__(self) -> AsyncIterator[Raw]:
        async def _gen() -> AsyncIterator[Raw]:
            if self._stream is None:
                raise ValueError("stream must be provided for SentimentStreamSource")
            async for msg in self._stream:
                yield msg

        return _gen()
