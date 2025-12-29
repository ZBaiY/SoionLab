

from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable, Awaitable

from ingestion.contracts.tick import IngestionTick
from ingestion.contracts.worker import IngestWorker

from ingestion.trades.normalize import BinanceAggTradesNormalizer


EmitFn = Callable[[IngestionTick], Any]


class TradesWorker(IngestWorker):
    """Generic trades ingestion worker.

    Responsibility:
        raw -> normalize -> emit tick

    Non-responsibilities:
        - time alignment / resampling
        - caching
        - backpressure policy (beyond cooperative yielding)
        - IO policy (delegated to `source`)
        - observation interval semantics (poll interval is IO-only)

    Source compatibility:
        - async sources: must implement `__aiter__` yielding raw payloads
        - sync sources : must implement `__iter__` yielding raw payloads

    Emit compatibility:
        - `emit(tick)` may be sync or async; if it returns an awaitable, we await it.
    """

    def __init__(
        self,
        *,
        normalizer: BinanceAggTradesNormalizer,
        source: Any,
        symbol: str,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
    ) -> None:
        self._normalizer = normalizer
        self._source = source
        self._symbol = str(symbol)

        if poll_interval_ms is not None:
            self._poll_interval_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            self._poll_interval_ms = 0
        if self._poll_interval_ms < 0:
            raise ValueError("poll_interval_ms must be >= 0")

    async def _emit(self, emit: EmitFn, tick: IngestionTick) -> None:
        try:
            out = emit(tick)
            if inspect.isawaitable(out):
                await out  # type: ignore[func-returns-value]
        except Exception:
            # Worker is intentionally dumb; let driver/logging decide policy.
            raise

    def _normalize(self, raw: Any) -> IngestionTick:
        # Normalizer already enforces ms-int time semantics.
        if isinstance(raw, dict):
            return self._normalizer.normalize(raw)
        # tolerate Mapping-like / pydantic-like objects
        try:
            return self._normalizer.normalize(dict(raw))  # type: ignore[arg-type]
        except Exception:
            # last resort: pass through
            return self._normalizer.normalize(raw)  # type: ignore[arg-type]

    async def run(self, emit: EmitFn) -> None:
        # --- async source ---
        if hasattr(self._source, "__aiter__"):
            async for raw in self._source:  # type: ignore
                tick = self._normalize(raw)
                await self._emit(emit, tick)
                # cooperative yield: avoid starving other tasks (e.g., driver loop)
                await asyncio.sleep(0)
            return

        # --- sync source ---
        if hasattr(self._source, "__iter__"):
            for raw in self._source:  # type: ignore
                tick = self._normalize(raw)
                await self._emit(emit, tick)
                if self._poll_interval_ms > 0:
                    await asyncio.sleep(self._poll_interval_ms / 1000.0)
                else:
                    await asyncio.sleep(0)
            return

        raise TypeError(
            "TradesWorker source must be an async-iterable or iterable; "
            f"got {type(self._source)!r}"
        )
