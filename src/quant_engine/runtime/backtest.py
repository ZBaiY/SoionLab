from __future__ import annotations

import asyncio
import threading
from typing import Any, AsyncIterator, Iterator

from ingestion.contracts.tick import IngestionTick
from quant_engine.runtime.driver import BaseDriver
from quant_engine.runtime.lifecycle import RuntimePhase
from quant_engine.runtime.snapshot import EngineSnapshot
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.contracts.engine import StrategyEngineProto
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.utils.asyncio import to_thread_limited
from quant_engine.utils.guards import ensure_epoch_ms
from quant_engine.utils.logger import log_debug, log_info, log_warn

DRAIN_YIELD_EVERY = 2048
STEP_LOG_EVERY = 100

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
        ingestion_tasks: list[asyncio.Task[None]] | None = None,
        drain_yield_every: int = DRAIN_YIELD_EVERY,
        step_log_every: int = STEP_LOG_EVERY,
        stop_event: threading.Event | None = None,
    ):
        super().__init__(engine=engine, spec=spec, stop_event=stop_event)
        self.start_ts = int(start_ts)
        self.end_ts = int(end_ts)
        self.tick_queue = tick_queue
        self._ingestion_tasks = ingestion_tasks
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
        if hasattr(item, "data_ts"):
            return ensure_epoch_ms(getattr(item, "data_ts"))
        if isinstance(item, dict) and "data_ts" in item:
            return ensure_epoch_ms(item["data_ts"])
        raise TypeError(f"Tick is missing 'data_ts': {type(item)!r}")
    
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
                log_info(self._logger, "driver.phase.step", timestamp=timestamp)
                # ---- ingest (optional gating) ----
                if getattr(self, "guard", None) is not None:
                    self.guard.enter(RuntimePhase.INGEST)

                # Align handlers before ingest to satisfy handler contracts.
                self.engine.align_to(timestamp)

                drained_ticks = 0
                async for tick in self.drain_ticks(until_timestamp=timestamp):
                    self.engine.ingest_tick(tick)
                    drained_ticks += 1

                if self.spec.mode == EngineMode.BACKTEST:
                    handler = None
                    if getattr(self.engine, "ohlcv_handlers", None):
                        handler = self.engine.ohlcv_handlers.get("BTCUSDT")
                        if handler is None:
                            handler = next(iter(self.engine.ohlcv_handlers.values()))
                    interval_ms = getattr(handler, "interval_ms", None) if handler is not None else None
                    if handler is not None and isinstance(interval_ms, int) and interval_ms > 0 and self.tick_queue is not None:
                        need_ts = (int(timestamp) // int(interval_ms)) * int(interval_ms) - 1
                        key = f"ohlcv:{handler.symbol}:{getattr(handler, 'source_id', None)}"
                        watermark = getattr(self.engine, "_last_tick_ts_by_key", {}).get(key)
                        if watermark is None and hasattr(handler, "last_timestamp"):
                            watermark = handler.last_timestamp()
                        while True:
                            if watermark is not None and int(watermark) >= int(need_ts):
                                break
                            if self._ingestion_tasks is not None and all(t.done() for t in self._ingestion_tasks):
                                raise RuntimeError(
                                    f"backtest.missing_data: key={key} need_ts={need_ts} watermark={watermark}"
                                )
                            raw = await self.tick_queue.get()
                            if isinstance(raw, tuple) and len(raw) == 2:
                                _ts, item = raw
                            elif isinstance(raw, tuple) and len(raw) == 3:
                                _ts, _seq, item = raw
                            else:
                                item = raw
                                _ts = getattr(item, "data_ts", None)
                            if _ts is not None and int(_ts) > int(timestamp):
                                raise RuntimeError(
                                    f"backtest.missing_data: key={key} need_ts={need_ts} head_ts={_ts} step_ts={timestamp}"
                                )
                            assert isinstance(item, IngestionTick)
                            self.engine.ingest_tick(item)
                            drained_ticks += 1
                            watermark = getattr(self.engine, "_last_tick_ts_by_key", {}).get(key)
                log_info(
                    self._logger,
                    "driver.ingest",
                    timestamp=timestamp,
                    drained_ticks_count=drained_ticks,
                )

                if self.spec.mode == EngineMode.BACKTEST:
                    handler = None
                    if getattr(self.engine, "ohlcv_handlers", None):
                        handler = self.engine.ohlcv_handlers.get("BTCUSDT")
                        if handler is None:
                            handler = next(iter(self.engine.ohlcv_handlers.values()))
                    interval_ms = getattr(handler, "interval_ms", None) if handler is not None else None
                    if handler is not None and isinstance(interval_ms, int) and interval_ms > 0:
                        expected_visible_end_ts = (int(timestamp) // int(interval_ms)) * int(interval_ms) - 1
                        actual_last_ts = handler.last_timestamp() if hasattr(handler, "last_timestamp") else None
                        closed_bar_ready = (
                            actual_last_ts is not None and int(actual_last_ts) >= int(expected_visible_end_ts)
                        )
                        if not closed_bar_ready:
                            log_warn(
                                self._logger,
                                "backtest.closed_bar.not_ready",
                                timestamp=int(timestamp),
                                expected_visible_end_ts=int(expected_visible_end_ts),
                                actual_last_ts=int(actual_last_ts) if actual_last_ts is not None else None,
                            )

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
