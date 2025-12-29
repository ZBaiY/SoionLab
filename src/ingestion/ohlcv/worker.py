from __future__ import annotations

import asyncio
import inspect
from typing import Callable, Awaitable



from ingestion.contracts.tick import IngestionTick, _to_interval_ms
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
        interval: str | None = None,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
    ):
        self._normalizer = normalizer
        self._source = source
        self._symbol = symbol
        self._interval = interval
        # Semantic bar interval length (ms-int). Used for metadata / validation.
        self.interval_ms: int | None = None
        if self._interval is not None:
            ms = _to_interval_ms(self._interval)
            if ms is None:
                raise ValueError(f"Invalid interval format: {self._interval!r}")
            self.interval_ms = int(ms)
        # Worker-level pacing for *sync* sources (file replay / REST wrappers).
        # Internal convention: ms-int.
        if poll_interval_ms is not None:
            self._poll_interval_ms: int | None = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        elif self.interval_ms is not None:
            # Default pacing for sync sources: 1 bar per interval.
            self._poll_interval_ms = int(self.interval_ms)
        else:
            self._poll_interval_ms = None

        if self._poll_interval_ms is not None and self._poll_interval_ms <= 0:
            raise ValueError(f"poll interval must be > 0ms, got {self._poll_interval_ms}")

    async def run(self, emit: Callable[[IngestionTick], Awaitable[None] | None]) -> None:

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
                if self._poll_interval_ms is not None:
                    await asyncio.sleep(self._poll_interval_ms / 1000.0)
                else:
                    # cooperative yield for fast iterators / file replay
                    await asyncio.sleep(0)

    def _normalize(self, raw: dict) -> IngestionTick:
        # symbol/domain knowledge lives in normalizer
        return self._normalizer.normalize(
            raw=raw,
        )
