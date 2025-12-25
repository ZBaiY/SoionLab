from __future__ import annotations

import asyncio
import inspect
from typing import Callable, Awaitable

from ingestion.contracts.tick import IngestionTick, _to_interval_ms
from ingestion.contracts.worker import IngestWorker
from ingestion.orderbook.normalize import BinanceOrderbookNormalizer
from ingestion.orderbook.source import OrderbookFileSource, OrderbookRESTSource, OrderbookWebSocketSource

class OrderbookWorker(IngestWorker):
    """
    Orderbook ingestion worker.
    This worker is source-agnostic and supports:
        - WebSocket streaming (AsyncSource)
        - REST polling / replay (Source)

    Responsibilities:
        raw -> normalize -> emit tick
    """

    def __init__(
        self,
        *,
        normalizer: BinanceOrderbookNormalizer,
        source: OrderbookFileSource | OrderbookRESTSource | OrderbookWebSocketSource,
        symbol: str,
        interval: str | None = None,
        interval_ms: int | None = None,
        poll_interval: float | None = None,
    ):
        self._normalizer = normalizer
        self._source = source
        self._symbol = symbol
        self._interval = interval
        if interval_ms is not None:
            self._interval_ms = interval_ms
        elif interval is not None:
            self._interval_ms = _to_interval_ms(interval)
        elif poll_interval is not None:
            self._interval_ms = int(round(poll_interval * 1000))
        else:
            self._interval_ms = None

    async def run(self, emit: Callable[[IngestionTick], Awaitable[None] | None]) -> None:
        async def _emit(tick: IngestionTick) -> None:
            r = emit(tick)
            if inspect.isawaitable(r):
                await r  # type: ignore[misc]

        # --- async source (e.g. WebSocket depth stream) ---
        if hasattr(self._source, "__aiter__"):
            async for raw in self._source:  # type: ignore
                tick = self._normalize(raw)
                await _emit(tick)
                await asyncio.sleep(0)

        # --- sync source (e.g. REST snapshot / backtest replay) ---
        else:
            for raw in self._source:  # type: ignore
                tick = self._normalize(raw)
                await _emit(tick)
                if self._interval_ms is not None:
                    await asyncio.sleep(self._interval_ms / 1000.0)
                else:
                    # cooperative yield to avoid starving other asyncio tasks
                    await asyncio.sleep(0)

    def _normalize(self, raw: dict) -> IngestionTick:
        return self._normalizer.normalize(raw=raw)
