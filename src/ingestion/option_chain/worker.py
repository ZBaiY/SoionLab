from __future__ import annotations

import asyncio
import inspect
from typing import Callable, Awaitable

from ingestion.contracts.tick import IngestionTick, normalize_tick
from ingestion.contracts.worker import IngestWorker
from ingestion.option_chain.normalize import DeribitOptionChainNormalizer
from ingestion.option_chain.source import (
    DeribitOptionChainRESTSource,
    OptionChainFileSource,
    OptionChainStreamSource,
)


class OptionChainWorker(IngestWorker):
    """
    Option chain ingestion worker.
    Responsibilities:
        raw -> normalize -> emit tick
    """

    def __init__(
        self,
        *,
        normalizer: DeribitOptionChainNormalizer,
        source: OptionChainFileSource | DeribitOptionChainRESTSource | OptionChainStreamSource,
        symbol: str,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
    ):
        self._normalizer = normalizer
        self._source = source
        self._symbol = symbol
        if poll_interval_ms is not None:
            self._poll_interval_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            self._poll_interval_ms = None
        if self._poll_interval_ms is not None and self._poll_interval_ms < 0:
            raise ValueError("poll_interval_ms must be >= 0")

    async def run(self, emit: Callable[[IngestionTick], Awaitable[None] | None]) -> None:
        async def _emit(tick: IngestionTick) -> None:
            r = emit(tick)
            if inspect.isawaitable(r):
                await r  # type: ignore[misc]

        # --- async source (streaming option updates) ---
        if hasattr(self._source, "__aiter__"):
            async for raw in self._source:  # type: ignore
                tick = self._normalize(raw)
                await _emit(tick)
                # Cooperative yield: prevent starvation when the stream is bursty
                await asyncio.sleep(0)

        # --- sync source (REST polling / file replay) ---
        else:
            for raw in self._source:  # type: ignore
                tick = self._normalize(raw)
                await _emit(tick)
                if self._poll_interval_ms is not None:
                    await asyncio.sleep(self._poll_interval_ms / 1000.0)
                else:
                    # Cooperative yield: prevent starvation for fast iterators
                    await asyncio.sleep(0)

    def _normalize(self, raw: dict) -> IngestionTick:
        payload = self._normalizer.normalize(raw)
        data_ts = payload.get("data_ts")
        if data_ts is None:
            raise ValueError("Option chain payload missing data_ts")
        return normalize_tick(
            timestamp=int(data_ts),
            data_ts=int(data_ts),
            domain="option_chain",
            symbol=self._symbol,
            payload=payload,
        )
