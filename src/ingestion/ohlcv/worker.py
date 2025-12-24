from __future__ import annotations

import asyncio
from typing import Callable, Iterable, AsyncIterable, Optional, Awaitable

from ingestion.contracts.tick import IngestionTick
from ingestion.contracts.worker import IngestWorker
from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer
from ingestion.ohlcv.source import OHLCVFileSource, OHLCVRESTSource, OHLCVWebSocketSource

class OHLCVWorker(IngestWorker):
    """
    Generic OHLCV ingestion worker.
    The only responsibility is:
        raw -> normalize -> emit tick
    Time, alignment, caching, and backpressure are NOT handled here.
    """

    def __init__(
        self,
        *,
        normalizer: BinanceOHLCVNormalizer,
        source: OHLCVFileSource | OHLCVRESTSource | OHLCVWebSocketSource,
        symbol: str,
        poll_interval: float | None = None,
    ):
        self._normalizer = normalizer
        self._source = source
        self._symbol = symbol
        self._poll_interval = poll_interval

    async def run(self, emit: Callable[[IngestionTick], Awaitable[None] | None]) -> None:
        import inspect

        async def _emit(tick: IngestionTick) -> None:
            r = emit(tick)
            if inspect.isawaitable(r):
                await r  # type: ignore[misc]

        # --- async source ---
        if hasattr(self._source, "__aiter__"):
            async for raw in self._source:  # type: ignore
                tick = self._normalize(raw)
                await _emit(tick)
                # cooperative yield: avoid starving other tasks (e.g., driver loop)
                await asyncio.sleep(0)
        # --- sync source (e.g. backtest iterator) ---
        else:
            for raw in self._source:  # type: ignore
                tick = self._normalize(raw)
                await _emit(tick)
                if self._poll_interval is not None:
                    await asyncio.sleep(self._poll_interval)
                else:
                    # cooperative yield for fast iterators / file replay
                    await asyncio.sleep(0)

    def _normalize(self, raw: dict) -> IngestionTick:
        # symbol/domain knowledge lives in normalizer
        return self._normalizer.normalize(
            raw=raw,
        )
