from __future__ import annotations

import json
import time
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ingestion.contracts.source import AsyncSource, Raw, Source
from ingestion.contracts.tick import _to_interval_ms


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

    # File pattern under provider. Supports either:
    #   - YYYY-MM-DD.jsonl
    #   - YYYY/MM/YYYY-MM-DD.jsonl
    pattern: str = "**/*.jsonl"


class SentimentFileSource(Source):
    """Sentiment source backed by local JSONL files.

    Recommended layout:
        <root>/sentiment/<provider>/
            2025-01-01.jsonl
            2025-01-02.jsonl
            ...

    Also allowed:
        <root>/sentiment/<provider>/YYYY/MM/2025-01-01.jsonl

    This Source yields one Raw record per JSONL line.
    """

    def __init__(
        self,
        *,
        root: str | Path,
        provider: str,
        pattern: str = "**/*.jsonl",
        strict: bool = True,
    ) -> None:
        self._layout = SentimentFileLayout(root=Path(root), provider=str(provider), pattern=str(pattern))
        self._strict = bool(strict)

        self._path = self._layout.root / self._layout.provider
        if self._strict and not self._path.exists():
            raise FileNotFoundError(f"Sentiment path does not exist: {self._path}")

    def __iter__(self) -> Iterator[Raw]:
        files = sorted(self._path.glob(self._layout.pattern)) if self._path.exists() else []
        if self._strict and not files:
            raise FileNotFoundError(f"No sentiment jsonl files found under {self._path}")

        for fp in files:
            if fp.is_dir():
                continue
            with fp.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    # Do not normalize; just parse JSON.
                    yield json.loads(line)


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
    ) -> None:
        self._fetch_fn = fetch_fn

        if interval_ms is not None:
            self._interval_ms = int(interval_ms)
        elif interval is not None:
            ms = _to_interval_ms(interval)
            if ms is None:
                raise ValueError(f"Invalid interval format: {interval!r}")
            self._interval_ms = int(ms)
        elif poll_interval is not None:
            # legacy compatibility
            self._interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            raise ValueError("One of interval_ms, interval, or poll_interval must be provided")

        if self._interval_ms <= 0:
            raise ValueError("interval_ms must be > 0")

    def __iter__(self) -> Iterator[Raw]:
        while True:
            rows = self._fetch_fn()
            for row in rows:
                yield row
            time.sleep(self._interval_ms / 1000.0)


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