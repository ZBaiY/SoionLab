from __future__ import annotations
import time
from typing import AsyncIterable, Iterator, AsyncIterator
from pathlib import Path
from ingestion.contracts.source import Source, AsyncSource, Raw


class OptionChainFileSource(Source):
    """
    Option chain source backed by local parquet files.

    Layout:
        root/
          └── <asset>/
              ├── chain_2025-01-01.parquet
              ├── chain_2025-01-02.parquet
              └── ...
    """

    def __init__(self, *, root: str | Path, **kwargs):
        self._root = Path(root)
        self._asset = kwargs.get("asset")
        assert isinstance(self._asset, str), "asset must be provided as a string"

        self._path = self._root / self._asset
        if not self._path.exists():
            raise FileNotFoundError(f"Option chain path does not exist: {self._path}")

    def __iter__(self) -> Iterator[Raw]:
        try:
            import pandas as pd
        except ImportError as e:
            raise RuntimeError("pandas is required for OptionChainFileSource parquet loading") from e

        files = sorted(self._path.glob("chain_*.parquet"))
        if not files:
            raise FileNotFoundError(f"No option chain parquet files found under {self._path}")

        for fp in files:
            df = pd.read_parquet(fp)
            for _, row in df.iterrows():
                yield row.to_dict()


class OptionChainRESTSource(Source):
    """
    Option chain source using REST-style polling.
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
            external callable returning an iterable of raw option chain payloads.
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


class OptionChainStreamSource(AsyncSource):
    """
    Option chain source backed by an async stream.
    """

    def __init__(self, stream: AsyncIterable[Raw] | None = None):
        self._stream = stream

    def __aiter__(self) -> AsyncIterator[Raw]:
        async def _gen():
            assert self._stream is not None, "stream must be provided for OptionChainStreamSource"
            async for msg in self._stream:
                yield msg

        return _gen()
