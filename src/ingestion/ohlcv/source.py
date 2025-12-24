

from __future__ import annotations

import time
from typing import Iterable, AsyncIterable, Iterator, AsyncIterator
from pathlib import Path

from ingestion.contracts.source import Source, AsyncSource, Raw


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

    def __init__(self, *, root: str | Path, **kwargs):
        self._root = Path(root)
        self._symbol = kwargs.get("symbol")
        self._interval = kwargs.get("interval") 
        assert isinstance(self._symbol, str), "symbol must be provided as a string"
        assert isinstance(self._interval, str), "interval must be provided as a string"
        self._path = self._root / self._symbol / self._interval
        if not self._path.exists():
            raise FileNotFoundError(f"OHLCV path does not exist: {self._path}")

    def __iter__(self) -> Iterator[Raw]:
        try:
            import pandas as pd
        except ImportError as e:
            raise RuntimeError("pandas is required for OHLCVFileSource parquet loading") from e

        files = sorted(self._path.glob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"No parquet files found under {self._path}")

        for fp in files:
            df = pd.read_parquet(fp)
            if "timestamp" not in df.columns:
                raise ValueError(f"Parquet file {fp} missing required 'timestamp' column")

            for _, row in df.iterrows():
                yield row.to_dict()


class OHLCVRESTSource(Source):
    """
    OHLCV source using REST-style polling.
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
            Callable returning an iterable of raw OHLCV payloads.
            The function itself handles authentication / pagination.
        """
        self._fetch_fn = fetch_fn
        self._poll_interval = poll_interval

    def __iter__(self) -> Iterator[Raw]:
        while True:
            rows = self._fetch_fn()
            for row in rows:
                yield row
            time.sleep(self._poll_interval)


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