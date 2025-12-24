
from __future__ import annotations

import time
from typing import AsyncIterable, Iterator, AsyncIterator
from pathlib import Path
from ingestion.contracts.source import Source, AsyncSource, Raw

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
        self._root = Path(root)
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
        poll_interval: float,
    ):
        """
        Parameters
        ----------
        fetch_fn:
            external callable returning an iterable of raw orderbook payloads.
            Authentication, pagination, retries, and vendor-specific logic live inside fetch_fn.
        """
        self._fetch_fn = fetch_fn
        self._poll_interval = poll_interval

    def __iter__(self) -> Iterator[Raw]:
        while True:
            rows = self._fetch_fn()
            for row in rows:
                yield row
            time.sleep(self._poll_interval)


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