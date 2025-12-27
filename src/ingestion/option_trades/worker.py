

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

from ingestion.contracts.tick import IngestionTick
from ingestion.contracts.worker import IngestWorker
from ingestion.option_trades.normalize import DeribitOptionTradesNormalizer
from ingestion.option_trades.source import (
    DeribitOptionTradesParquetSource,
    DeribitOptionTradesRESTSource,
)


EmitFn = Callable[[IngestionTick], None] | Callable[[IngestionTick], Awaitable[None]]


class _FetchLike(Protocol):
    def fetch(self) -> Any:  # may return iterable, or awaitable of iterable
        ...


@dataclass
class OptionTradesWorker(IngestWorker):
    """Option trades ingestion worker.

    Responsibility: raw -> normalize -> emit tick

    Notes:
      - Source may be:
          * (async) iterator yielding raw dict-like objects
          * an object exposing `fetch()` returning (or awaiting) an iterable of raw dict-like objects
      - This worker is IO-agnostic; polling cadence is optional and only used for `fetch()` sources.
    """

    normalizer: DeribitOptionTradesNormalizer
    source: DeribitOptionTradesRESTSource | DeribitOptionTradesParquetSource | _FetchLike | Any

    symbol: str
    poll_interval_s: float | None = None

    def __init__(
        self,
        *,
        normalizer: DeribitOptionTradesNormalizer,
        source: DeribitOptionTradesRESTSource | DeribitOptionTradesParquetSource | Any,
        symbol: str,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
    ) -> None:
        self.normalizer = normalizer
        self.source = source
        self.symbol = str(symbol)

        if poll_interval_ms is not None:
            self.poll_interval_s = float(poll_interval_ms) / 1000.0
        else:
            self.poll_interval_s = float(poll_interval) if poll_interval is not None else None

    def _normalize(self, raw: Any) -> IngestionTick:
        # Normalizer is the only place allowed to interpret source schema.
        return self.normalizer.normalize(raw)

    async def _emit(self, emit: EmitFn, tick: IngestionTick) -> None:
        res = emit(tick)  # type: ignore[misc]
        if inspect.isawaitable(res):
            await res  # type: ignore[func-returns-value]

    async def run(self, emit: EmitFn) -> None:
        src = self.source

        # --- async iterator source ---
        if hasattr(src, "__aiter__"):
            async for raw in src:  # type: ignore[assignment]
                tick = self._normalize(raw)
                await self._emit(emit, tick)
                # cooperative yield
                await asyncio.sleep(0)
            return

        # --- fetch()-style source (optionally polled) ---
        if hasattr(src, "fetch") and callable(getattr(src, "fetch")):
            while True:
                batch = src.fetch()  # type: ignore[attr-defined]
                if inspect.isawaitable(batch):
                    batch = await batch

                any_row = False
                for raw in (batch or []):
                    any_row = True
                    tick = self._normalize(raw)
                    await self._emit(emit, tick)

                if self.poll_interval_s is None:
                    # one-shot fetch
                    return

                # if the fetch returned nothing, still sleep (avoid hot loop)
                await asyncio.sleep(max(0.0, float(self.poll_interval_s)))
            
        # --- sync iterator source ---
        for raw in src:  # type: ignore[assignment]
            tick = self._normalize(raw)
            await self._emit(emit, tick)
            await asyncio.sleep(0)