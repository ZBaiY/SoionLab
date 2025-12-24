from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Iterator

from quant_engine.runtime.driver import BaseDriver
from quant_engine.runtime.lifecycle import RuntimePhase
from quant_engine.runtime.snapshot import EngineSnapshot
from quant_engine.runtime.modes import EngineSpec
from quant_engine.strategy.engine import StrategyEngine


class BacktestDriver(BaseDriver):
    """Deterministic backtest driver.

    BacktestDriver provides deterministic time progression, and optionally supports
    driver-gated tick ingestion ("口径2") via an in-memory priority queue.
    """

    def __init__(
        self,
        *,
        engine: StrategyEngine,
        spec: EngineSpec,
        start_ts: float,
        end_ts: float,
        tick_queue: asyncio.PriorityQueue[Any] | None = None,
    ):
        super().__init__(engine=engine, spec=spec)
        self.start_ts = float(start_ts)
        self.end_ts = float(end_ts)

        # Optional: driver-gated ingestion (口径2). If provided, the driver drains
        # ticks up to each step timestamp via iter_ticks().
        self.tick_queue = tick_queue
        self._next_tick: Any | None = None
        self._snapshots: list[EngineSnapshot] = []


    async def iter_timestamps(self) -> AsyncIterator[float]:
        """Yield engine-time timestamps in strictly increasing order."""
        current = self.start_ts
        end = self.end_ts
        while current <= end:
            yield current
            current = self.spec.advance(current)
            # Cooperative scheduling point (lets ingestion tasks run in the same loop)
            await asyncio.sleep(0)

    async def run(self) -> None:
        """Canonical v4 runtime loop:

        PRELOAD -> WARMUP -> (INGEST -> STEP)* -> FINISH
        """

        # -------- preload --------
        if getattr(self, "guard", None) is not None:
            self.guard.enter(RuntimePhase.PRELOAD)
        self.engine.preload_data(anchor_ts=self.spec.timestamp)

        # -------- warmup --------
        if getattr(self, "guard", None) is not None:
            self.guard.enter(RuntimePhase.WARMUP)
        self.engine.warmup_features(anchor_ts=self.spec.timestamp)

        # -------- main loop --------
        async for ts in self.iter_timestamps():
            timestamp = float(ts)

            # ---- ingest (optional gating) ----
            if getattr(self, "guard", None) is not None:
                self.guard.enter(RuntimePhase.INGEST)

            for tick in self.iter_ticks(until_timestamp=timestamp):
                self.engine.ingest_tick(tick)

            # ---- step ----
            if getattr(self, "guard", None) is not None:
                self.guard.enter(RuntimePhase.STEP)

            self.engine.align_to(timestamp)

            # If your engine.step arg name is still `ts`, keep ts=timestamp.
            result = self.engine.step(ts=timestamp)

            # Give other tasks / callbacks a chance to run.
            await asyncio.sleep(0)

            # ---- optional snapshots ----
            if result is None:
                continue

            if not hasattr(self, "_snapshots"):
                self._snapshots = []

            if isinstance(result, EngineSnapshot):
                self._snapshots.append(result)
            elif isinstance(result, dict):
                # Avoid getattr() here so static type checkers don't treat this as `object`.
                # Also avoid passing `timestamp` twice if engine.step() already returned it.
                payload = dict(result)
                payload.pop("timestamp", None)
                self._snapshots.append(self.engine.engine_snapshot)

        # -------- finish --------
        if getattr(self, "guard", None) is not None:
            self.guard.enter(RuntimePhase.FINISH)

    def _extract_tick_timestamp(self, item: Any) -> float:
        """Extract timestamp from either a Tick-like object or dict payload."""
        if hasattr(item, "timestamp"):
            return float(getattr(item, "timestamp"))
        if isinstance(item, dict) and "timestamp" in item:
            return float(item["timestamp"])
        raise TypeError(f"Tick is missing 'timestamp': {type(item)!r}")
    def iter_ticks(self, *, until_timestamp: float) -> Iterator[Any]:
        """Yield ticks with tick.timestamp <= until_timestamp.

        Any tick with timestamp > until_timestamp is buffered and will be yielded
        on a later call.
        """
        if self.tick_queue is None:
            return
            yield  # pragma: no cover

        until = float(until_timestamp)

        # Flush buffered tick first.
        if self._next_tick is not None:
            item = self._next_tick
            self._next_tick = None
            ts = self._extract_tick_timestamp(item)
            if ts <= until:
                yield item
            else:
                self._next_tick = item
                return

        # Drain queue (non-blocking) up to `until`.
        while True:
            try:
                raw = self.tick_queue.get_nowait()
            except asyncio.QueueEmpty:
                return

            # Normalize queue item shapes.
            item: Any
            if isinstance(raw, tuple) and len(raw) == 2:
                ts, item = raw
                ts = float(ts)
            elif isinstance(raw, tuple) and len(raw) == 3:
                ts, _seq, item = raw
                ts = float(ts)
            else:
                item = raw
                ts = self._extract_tick_timestamp(item)

            if ts <= until:
                yield item
            else:
                self._next_tick = item
                return