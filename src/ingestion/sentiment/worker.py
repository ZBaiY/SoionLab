from __future__ import annotations

import asyncio
from typing import Callable, Awaitable

from ingestion.contracts.tick import IngestionTick
from ingestion.contracts.worker import IngestWorker
from ingestion.sentiment.normalize import GenericSentimentNormalizer
from ingestion.sentiment.source import SentimentFileSource, SentimentRESTSource, SentimentStreamSource

class SentimentWorker(IngestWorker):
    """
    Sentiment ingestion worker.
    """

    def __init__(
        self,
        *,
        normalizer: GenericSentimentNormalizer,
        source: SentimentFileSource | SentimentRESTSource | SentimentStreamSource,
        symbol: str,
        poll_interval: float | None = None,
    ):
        self._normalizer = normalizer
        self._source = source
        self._symbol = symbol
        self._poll_interval = poll_interval

    async def run(
        self,
        emit: Callable[[IngestionTick], Awaitable[None] | None],
    ) -> None:
        async def _emit(tick: IngestionTick) -> None:
            res = emit(tick)
            if asyncio.iscoroutine(res) or isinstance(res, asyncio.Future):
                await res

        # --- async source (e.g. streaming sentiment feed) ---
        if hasattr(self._source, "__aiter__"):
            async for raw in self._source:  # type: ignore
                tick = self._normalize(raw)
                await _emit(tick)
                await asyncio.sleep(0)

        # --- sync source (e.g. REST polling / file replay) ---
        else:
            for raw in self._source:  # type: ignore
                tick = self._normalize(raw)
                await _emit(tick)
                if self._poll_interval is not None:
                    await asyncio.sleep(self._poll_interval)
                else:
                    await asyncio.sleep(0)

    def _normalize(self, raw: dict) -> IngestionTick:
        return self._normalizer.normalize(raw=raw)