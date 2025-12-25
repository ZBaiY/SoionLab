from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Iterator

from quant_engine.runtime.driver import BaseDriver
from quant_engine.runtime.lifecycle import RuntimePhase
from quant_engine.runtime.snapshot import EngineSnapshot
from quant_engine.runtime.modes import EngineSpec
from quant_engine.strategy.engine import StrategyEngine

DRAIN_YIELD_EVERY = 2048

class BacktestDriver(BaseDriver):
    """Deterministic backtest driver with optional driver-gated ingestion ("口径2")."""

    def __init__(
        self,
        *,
        engine: StrategyEngine,
        spec: EngineSpec,
        start_ts: int,
        end_ts: int,
        tick_queue: asyncio.PriorityQueue[Any] | None = None,
        drain_yield_every: int = DRAIN_YIELD_EVERY,
    ):
        super().__init__(engine=engine, spec=spec)
        self.start_ts = int(start_ts)
        self.end_ts = int(end_ts)
        self.tick_queue = tick_queue
        self._next_tick: Any | None = None
        self._snapshots: list[EngineSnapshot] = []
        self._drain_yield_every = int(drain_yield_every)

    async def iter_timestamps(self) -> AsyncIterator[int]:
        current = self.start_ts
        end = self.end_ts
        while current <= end:
            yield current
            current = self.spec.advance(current)
            await asyncio.sleep(0)  # cooperative scheduling point

    def _extract_tick_timestamp(self, item: Any) -> int:
        if hasattr(item, "timestamp"):
            return int(getattr(item, "timestamp"))
        if isinstance(item, dict) and "timestamp" in item:
            return int(item["timestamp"])
        raise TypeError(f"Tick is missing 'timestamp': {type(item)!r}")

    async def drain_ticks(self, *, until_timestamp: int) -> AsyncIterator[Any]:
        """Yield ticks with tick.timestamp <= until_timestamp (cooperatively)."""
        if self.tick_queue is None:
            return

        until = int(until_timestamp)
        n = 0

        # flush buffered tick first
        if self._next_tick is not None:
            item = self._next_tick
            self._next_tick = None
            ts = self._extract_tick_timestamp(item)
            if ts <= until:
                yield item
                n += 1
            else:
                self._next_tick = item
                return

        while True:
            try:
                raw = self.tick_queue.get_nowait()
            except asyncio.QueueEmpty:
                return

            # normalize queue item shapes
            if isinstance(raw, tuple) and len(raw) == 2:
                ts, item = raw
                ts = int(ts)
            elif isinstance(raw, tuple) and len(raw) == 3:
                ts, _seq, item = raw
                ts = int(ts)
            else:
                item = raw
                ts = self._extract_tick_timestamp(item)

            if ts <= until:
                yield item
                n += 1
                if self._drain_yield_every > 0 and (n % self._drain_yield_every) == 0:
                    await asyncio.sleep(0)  # don’t hog the loop on huge queues
            else:
                self._next_tick = item
                return

    async def run(self) -> None:
        # -------- preload --------
        if getattr(self, "guard", None) is not None:
            self.guard.enter(RuntimePhase.PRELOAD)
        self.engine.preload_data(anchor_ts=self.start_ts)

        # -------- warmup --------
        if getattr(self, "guard", None) is not None:
            self.guard.enter(RuntimePhase.WARMUP)
        self.engine.warmup_features(anchor_ts=self.start_ts)

        # -------- main loop --------
        async for ts in self.iter_timestamps():
            timestamp = int(ts)

            # ---- ingest (optional gating) ----
            if getattr(self, "guard", None) is not None:
                self.guard.enter(RuntimePhase.INGEST)

            async for tick in self.drain_ticks(until_timestamp=timestamp):
                self.engine.ingest_tick(tick)

            # ---- step ----
            if getattr(self, "guard", None) is not None:
                self.guard.enter(RuntimePhase.STEP)

            self.engine.align_to(timestamp)
            result = self.engine.step(ts=timestamp)

            await asyncio.sleep(0)  # let ingestion tasks run

            if isinstance(result, EngineSnapshot):
                self._snapshots.append(result)
            elif isinstance(result, dict):
                self._snapshots.append(self.engine.engine_snapshot)

        # -------- finish --------
        if getattr(self, "guard", None) is not None:
            self.guard.enter(RuntimePhase.FINISH)
