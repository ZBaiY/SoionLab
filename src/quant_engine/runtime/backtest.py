from __future__ import annotations

import asyncio
import threading
from typing import Any, AsyncIterator, Iterator

from quant_engine.runtime.driver import BaseDriver
from quant_engine.runtime.lifecycle import RuntimePhase
from quant_engine.runtime.snapshot import EngineSnapshot
from quant_engine.runtime.modes import EngineSpec
from quant_engine.contracts.engine import StrategyEngineProto
from quant_engine.utils.asyncio import to_thread_limited
from quant_engine.utils.guards import ensure_epoch_ms
from quant_engine.utils.logger import log_debug, log_info

DRAIN_YIELD_EVERY = 2048
STEP_LOG_EVERY = 100

class BacktestDriver(BaseDriver):
    """Deterministic backtest driver with optional driver-gated ingestion ("口径2")."""

    def __init__(
        self,
        *,
        engine: StrategyEngineProto,
        spec: EngineSpec,
        start_ts: int,
        end_ts: int,
        tick_queue: asyncio.PriorityQueue[Any] | None = None,
        drain_yield_every: int = DRAIN_YIELD_EVERY,
        step_log_every: int = STEP_LOG_EVERY,
        stop_event: threading.Event | None = None,
    ):
        super().__init__(engine=engine, spec=spec, stop_event=stop_event)
        self.start_ts = int(start_ts)
        self.end_ts = int(end_ts)
        self.tick_queue = tick_queue
        self._next_tick: Any | None = None
        self._snapshots: list[EngineSnapshot] = []
        self._drain_yield_every = int(drain_yield_every)
        self._step_log_every = int(step_log_every)

    async def iter_timestamps(self) -> AsyncIterator[int]:
        current = self.start_ts
        end = self.end_ts
        while current <= end:
            yield current
            current = self.spec.advance(current)
            await asyncio.sleep(0)  # cooperative scheduling point

    def _extract_tick_timestamp(self, item: Any) -> int:
        if hasattr(item, "timestamp"):
            return ensure_epoch_ms(getattr(item, "timestamp"))
        if isinstance(item, dict) and "timestamp" in item:
            return ensure_epoch_ms(item["timestamp"])
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
                ts = ensure_epoch_ms(ts)
            elif isinstance(raw, tuple) and len(raw) == 3:
                ts, _seq, item = raw
                ts = ensure_epoch_ms(ts)
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
        self._install_loop_exception_handler()
        try:
            # -------- preload --------
            if getattr(self, "guard", None) is not None:
                self.guard.enter(RuntimePhase.PRELOAD)
            log_info(self._logger, "driver.phase.load_history", timestamp=self.start_ts)
            await to_thread_limited(
                self.engine.load_history,
                start_ts=self.start_ts,
                end_ts=self.end_ts,
                logger=self._logger,
                context={"driver": self.__class__.__name__},
                op="load_history",
            )

            # -------- warmup --------
            if getattr(self, "guard", None) is not None:
                self.guard.enter(RuntimePhase.WARMUP)
            log_info(self._logger, "driver.phase.warmup", timestamp=self.start_ts)
            await to_thread_limited(
                self.engine.warmup_features,
                anchor_ts=self.start_ts,
                logger=self._logger,
                context={"driver": self.__class__.__name__},
                op="warmup_features",
            )

            # -------- main loop --------
            step_count = 0
            async for ts in self.iter_timestamps():
                if self.stop_event.is_set():
                    break
                timestamp = int(ts)

                # ---- ingest (optional gating) ----
                if getattr(self, "guard", None) is not None:
                    self.guard.enter(RuntimePhase.INGEST)

                # Align handlers before ingest to satisfy handler contracts.
                self.engine.align_to(timestamp)

                drained_ticks = 0
                async for tick in self.drain_ticks(until_timestamp=timestamp):
                    self.engine.ingest_tick(tick)
                    drained_ticks += 1

                # ---- step ----
                if getattr(self, "guard", None) is not None:
                    self.guard.enter(RuntimePhase.STEP)

                result = self.engine.step(ts=timestamp)

                await asyncio.sleep(0)  # let ingestion tasks run

                if isinstance(result, EngineSnapshot):
                    self._snapshots.append(result)
                elif isinstance(result, dict):
                    snap = self.engine.get_snapshot() if hasattr(self.engine, "get_snapshot") else None
                    if snap is not None:
                        self._snapshots.append(snap)
                step_count += 1
                if self._step_log_every > 0 and (step_count % self._step_log_every) == 0:
                    log_debug(
                        self._logger,
                        "driver.step",
                        timestamp=timestamp,
                        drained_ticks_count=drained_ticks,
                        snapshots_len=len(self._snapshots),
                    )

            # -------- finish --------
            if getattr(self, "guard", None) is not None:
                self.guard.enter(RuntimePhase.FINISH)
            log_info(self._logger, "driver.phase.finish", timestamp=self.end_ts)
        except asyncio.CancelledError:
            self._shutdown_components()
            raise
        except Exception as exc:
            self._handle_fatal(exc)
        finally:
            await self._cancel_background_tasks()
