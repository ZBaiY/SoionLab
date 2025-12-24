from __future__ import annotations

import asyncio
import inspect
from typing import Callable, Awaitable

from ingestion.contracts.tick import IngestionTick
from ingestion.contracts.worker import IngestWorker
from ingestion.option_chain.normalize import GenericOptionChainNormalizer
from ingestion.option_chain.source import (
    OptionChainFileSource,
    OptionChainRESTSource,
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
        normalizer: GenericOptionChainNormalizer,
        source: OptionChainFileSource | OptionChainRESTSource | OptionChainStreamSource,
        symbol: str,
        poll_interval: float | None = None,
    ):
        self._normalizer = normalizer
        self._source = source
        self._symbol = symbol
        self._poll_interval = poll_interval

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
                if self._poll_interval is not None:
                    # Poll pacing (REST / file replay with a target cadence)
                    await asyncio.sleep(self._poll_interval)
                else:
                    # Cooperative yield: prevent starvation for fast iterators
                    await asyncio.sleep(0)

    def _normalize(self, raw: dict) -> IngestionTick:
        return self._normalizer.normalize(raw=raw)
