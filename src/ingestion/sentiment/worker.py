from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from ingestion.contracts.tick import IngestionTick, _to_interval_ms
from ingestion.contracts.worker import IngestWorker
from ingestion.sentiment.normalize import SentimentNormalizer
from ingestion.sentiment.source import (
    SentimentFileSource,
    SentimentRESTSource,
    SentimentStreamSource,
)


class SentimentWorker(IngestWorker):
    """Sentiment ingestion worker.

    Responsibilities:
      - raw -> normalize -> emit tick
      - provide arrival_ts (ingestion timestamp) to the normalizer

    Non-responsibilities:
      - scoring (VADER/FinBERT)
      - alignment/backpressure
    """

    def __init__(
        self,
        *,
        normalizer: SentimentNormalizer,
        source: SentimentFileSource | SentimentRESTSource | SentimentStreamSource,
        interval: str | None = None,
        interval_ms: int | None = None,
        poll_interval: float | None = None,
    ) -> None:
        self._normalizer = normalizer
        self._source = source

        # Sleep pacing is only relevant for sync sources.
        if interval_ms is not None:
            self._interval_ms: int | None = int(interval_ms)
        elif interval is not None:
            ms = _to_interval_ms(interval)
            if ms is None:
                raise ValueError(f"Invalid interval format: {interval!r}")
            self._interval_ms = int(ms)
        elif poll_interval is not None:
            # legacy compatibility
            self._interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            self._interval_ms = None

        if self._interval_ms is not None and self._interval_ms <= 0:
            raise ValueError("interval_ms must be > 0")

    async def run(
        self,
        emit: Callable[[IngestionTick], Awaitable[None] | None],
    ) -> None:
        async def _emit(tick: IngestionTick) -> None:
            res = emit(tick)
            if asyncio.iscoroutine(res) or isinstance(res, asyncio.Future):
                await res

        def _now_ms() -> int:
            return int(time.time() * 1000)

        # --- async source (e.g. streaming sentiment feed) ---
        if hasattr(self._source, "__aiter__"):
            async for raw in self._source:  # type: ignore
                tick = self._normalizer.normalize(raw=raw, arrival_ts=_now_ms())
                await _emit(tick)
                await asyncio.sleep(0)

        # --- sync source (e.g. REST polling / file replay) ---
        else:
            for raw in self._source:  # type: ignore
                tick = self._normalizer.normalize(raw=raw, arrival_ts=_now_ms())
                await _emit(tick)

                if self._interval_ms is not None:
                    await asyncio.sleep(self._interval_ms / 1000.0)
                else:
                    await asyncio.sleep(0)