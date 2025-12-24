from __future__ import annotations
import time
import json
from typing import AsyncIterable, Iterator, AsyncIterator
from pathlib import Path
from ingestion.contracts.source import Source, AsyncSource, Raw


class SentimentFileSource(Source):
    """
    Sentiment source backed by local JSONL files.

    Layout:
        root/
          └── <source>/
              ├── 2025-01-01.jsonl
              ├── 2025-01-02.jsonl
              └── ...
    """

    def __init__(self, *, root: str | Path, source: str):
        self._root = Path(root)
        self._source = source

        self._path = self._root / source
        if not self._path.exists():
            raise FileNotFoundError(f"Sentiment path does not exist: {self._path}")

    def __iter__(self) -> Iterator[Raw]:
        files = sorted(self._path.glob("*.jsonl"))
        if not files:
            raise FileNotFoundError(f"No sentiment jsonl files found under {self._path}")

        for fp in files:
            with fp.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    yield json.loads(line)


class SentimentRESTSource(Source):
    """
    Sentiment source using REST-style polling.
    """

    def __init__(
        self,
        *,
        fetch_fn,
        poll_interval: float,
    ):
        """
        Parameters
        ----------
        fetch_fn:
            external callable returning an iterable of raw sentiment payloads.
            Authentication, pagination, retries, and vendor-specific logic live inside fetch_fn.
        poll_interval:
            Sleep interval between polls (seconds).
        """
        self._fetch_fn = fetch_fn
        self._poll_interval = poll_interval

    def __iter__(self) -> Iterator[Raw]:
        while True:
            rows = self._fetch_fn()
            for row in rows:
                yield row
            time.sleep(self._poll_interval)


class SentimentStreamSource(AsyncSource):
    """
    Sentiment source backed by an async stream.
    """

    def __init__(self, stream: AsyncIterable[Raw] | None = None):
        self._stream = stream

    def __aiter__(self) -> AsyncIterator[Raw]:
        async def _gen():
            assert self._stream is not None, "stream must be provided for SentimentStreamSource"
            async for msg in self._stream:
                yield msg

        return _gen()