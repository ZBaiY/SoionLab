from __future__ import annotations

import asyncio
import threading
from typing import Any, AsyncIterator

from ingestion.contracts.tick import IngestionTick
from quant_engine.runtime.driver import BaseDriver
from quant_engine.runtime.lifecycle import RuntimePhase
from quant_engine.runtime.snapshot import EngineSnapshot
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.strategy.engine import StrategyEngine, format_tick_key
from quant_engine.utils.asyncio import to_thread_limited
from quant_engine.utils.guards import ensure_epoch_ms
from quant_engine.utils.logger import log_debug, log_info, log_warn, log_throttle, throttle_key
from quant_engine.utils.num import visible_end_ts

DRAIN_YIELD_EVERY = 2048
STEP_LOG_EVERY = 100
OPTION_CHAIN_IDLE_WAIT_ROUNDS = 8
OPTION_CHAIN_IDLE_WAIT_SLEEP_S = 0.05
OPTION_CHAIN_NOT_ADMITTED_LOG_THROTTLE_S = 1.0

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
            # Driver is the single time authority and sequences lifecycle transitions.
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
            # Driver timestamp is the anchor.
            self.guard.enter(RuntimePhase.WARMUP)
            log_info(self._logger, "driver.phase.warmup", timestamp=self.start_ts)
            await to_thread_limited(
                self.engine.warmup_features,
                anchor_ts=self.start_ts,
                logger=self._logger,
                context={"driver": self.__class__.__name__},
                op="warmup_features",
            )


            required_ohlcv_handlers: list[tuple[Any, int, str]] = []
            option_chain_handler = None
            option_chain_symbol = None
            option_chain_interval_ms: int | None = None
            if self.spec.mode in (EngineMode.BACKTEST, EngineMode.SAMPLE):
                handlers = getattr(self.engine, "ohlcv_handlers", {}) or {}
                for symbol in handlers.keys():
                    handler = handlers.get(symbol)
                    if handler is None:
                        raise RuntimeError(f"backtest.missing_handler: domain=ohlcv symbol={symbol}")
                    interval_ms = getattr(handler, "interval_ms", None)
                    if not isinstance(interval_ms, int) or interval_ms <= 0:
                        raise RuntimeError(
                            f"backtest.missing_handler: domain=ohlcv symbol={symbol} interval_ms={interval_ms}"
                        )
                    key = format_tick_key("ohlcv", handler.symbol, getattr(handler, "source_id", None))
                    required_ohlcv_handlers.append((handler, int(interval_ms), key))

                option_handlers = getattr(self.engine, "option_chain_handlers", {}) or {}
                if option_handlers:
                    primary_symbol = getattr(self.engine, "symbol", None)
                    option_chain_handler = option_handlers.get(primary_symbol) if primary_symbol is not None else None
                    if option_chain_handler is None:
                        option_chain_handler = next(iter(option_handlers.values()))
                    option_chain_symbol = getattr(option_chain_handler, "symbol", primary_symbol)
                    interval_val = getattr(option_chain_handler, "interval_ms", None)
                    if not isinstance(interval_val, int) or interval_val <= 0:
                        raise RuntimeError(
                            f"backtest.missing_handler: domain=option_chain symbol={option_chain_symbol} interval_ms={interval_val}"
                        )
                    option_chain_interval_ms = int(interval_val)

            step_count = 0
            # Driver is the single time authority; iter_timestamps defines step anchors.
            async for ts in self.iter_timestamps():
                if self.stop_event.is_set():
                    break
                timestamp = int(ts)
                log_debug(self._logger, "driver.phase.step", timestamp=timestamp)
                # ---- ingest (optional gating) ----
                self.guard.enter(RuntimePhase.INGEST)

                # Driver timestamp is the anchor.
                # Align handlers before ingest to satisfy handler contracts.
                self.engine.align_to(timestamp)

                drained_ticks = 0
                async for tick in self.drain_ticks(until_timestamp=timestamp):
                    self.engine.ingest_tick(tick)
                    drained_ticks += 1

                if self.spec.mode in (EngineMode.BACKTEST, EngineMode.SAMPLE):
                    if required_ohlcv_handlers and self.tick_queue is not None:
                        required_keys = {key for _handler, _interval_ms, key in required_ohlcv_handlers}
                        while True:
                            last_tick_ts_by_key = getattr(self.engine, "_last_tick_ts_by_key", {}) or {}
                            not_ready: list[tuple[Any, int, str, int | None]] = []
                            not_ready_by_key: dict[str, tuple[Any, int, int | None]] = {}
                            for handler, interval_ms, key in required_ohlcv_handlers:
                                need_ts = visible_end_ts(timestamp, interval_ms)
                                watermark = last_tick_ts_by_key.get(key)
                                if watermark is None and callable(getattr(handler, "last_timestamp", None)):
                                    watermark = handler.last_timestamp()
                                if watermark is None or int(watermark) < int(need_ts):
                                    not_ready.append((handler, int(need_ts), key, watermark))
                                    not_ready_by_key[key] = (handler, int(need_ts), watermark)
                            if not not_ready:
                                break
                            if self._ingestion_tasks is not None and all(t.done() for t in self._ingestion_tasks):
                                details = [
                                    f"{getattr(h, 'symbol', None)}:{need_ts}:{wm}"
                                    for h, need_ts, _key, wm in not_ready
                                ]
                                raise RuntimeError(
                                    f"backtest.missing_data: step_ts={timestamp} missing={details}"
                                )
                            drained_any = False
                            while True:
                                head = getattr(self.tick_queue, "_queue", None)
                                head_item = head[0] if head else None
                                head_ts = head_item[0] if head_item else None
                                if head_ts is None or int(head_ts) > int(timestamp):
                                    break
                                raw = await self.tick_queue.get()
                                if isinstance(raw, tuple) and len(raw) == 2:
                                    _ts, item = raw
                                elif isinstance(raw, tuple) and len(raw) == 3:
                                    _ts, _seq, item = raw
                                else:
                                    item = raw
                                assert isinstance(item, IngestionTick)
                                self.engine.ingest_tick(item)
                                drained_ticks += 1
                                drained_any = True
                            if not drained_any:
                                head = getattr(self.tick_queue, "_queue", None)
                                head_item = head[0] if head else None
                                head_ts = head_item[0] if head_item else None
                                head_tick = None
                                if isinstance(head_item, tuple) and len(head_item) >= 2:
                                    head_tick = head_item[-1]
                                head_key = None
                                if isinstance(head_tick, IngestionTick):
                                    head_key = format_tick_key(
                                        head_tick.domain,
                                        head_tick.symbol,
                                        getattr(head_tick, "source_id", None),
                                    )
                                if (
                                    head_ts is not None
                                    and int(head_ts) > int(timestamp)
                                    and head_key in required_keys
                                    and head_key in not_ready_by_key
                                ):
                                    handler, need_ts, _wm = not_ready_by_key[head_key]
                                    watermark_engine = last_tick_ts_by_key.get(head_key)
                                    watermark_handler = (
                                        handler.last_timestamp()
                                        if callable(getattr(handler, "last_timestamp", None))
                                        else None
                                    )
                                    raise RuntimeError(
                                        "backtest.missing_data"
                                        f": step_ts={timestamp}"
                                        f" key={head_key}"
                                        f" need_ts={need_ts}"
                                        f" head_ts={head_ts}"
                                        f" watermark_from_engine={watermark_engine}"
                                        f" watermark_from_handler={watermark_handler}"
                                    )
                                await asyncio.sleep(0)
                        for handler, interval_ms, _key in required_ohlcv_handlers:
                            need_ts = visible_end_ts(timestamp, interval_ms)
                            actual_last_ts = handler.last_timestamp() if hasattr(handler, "last_timestamp") else None
                            closed_bar_ready = actual_last_ts is not None and int(actual_last_ts) >= int(need_ts)
                            if not closed_bar_ready:
                                symbol = getattr(handler, "symbol", None)
                                handler_type = handler.__class__.__name__
                                interval_val = getattr(handler, "interval", None) or getattr(handler, "interval_ms", None)
                                key = throttle_key("backtest.closed_bar.not_ready", handler_type, symbol, interval_val)
                                lag_ms = int(need_ts) - int(actual_last_ts) if actual_last_ts is not None else None
                                interval_ms = int(interval_ms) if isinstance(interval_ms, int) else None
                                actionable = (
                                    lag_ms is not None
                                    and interval_ms is not None
                                    and lag_ms >= 3 * int(interval_ms)
                                )
                                if log_throttle(key, 60.0):
                                    log_fn = log_warn if actionable else log_debug
                                    log_fn(
                                        self._logger,
                                        "backtest.closed_bar.not_ready",
                                        timestamp=int(timestamp),
                                        expected_visible_end_ts=int(need_ts),
                                        actual_last_ts=int(actual_last_ts) if actual_last_ts is not None else None,
                                        symbol=symbol,
                                        lag_ms=lag_ms,
                                        interval_ms=interval_ms,
                                    )
                    option_chain_ctx = None
                    if option_chain_handler is not None and option_chain_interval_ms is not None:
                        allowed_lag_ms = int(option_chain_interval_ms) * 2
                        required_ts = int(timestamp) - int(allowed_lag_ms)
                        idle_wait_rounds = 0
                        total_wait_rounds = 0
                        wait_state = "waiting"
                        wait_exhausted = False
                        procedural_stop = None
                        while True:
                            latest_visible_ts = (
                                option_chain_handler.last_timestamp()
                                if callable(getattr(option_chain_handler, "last_timestamp", None))
                                else None
                            )
                            admitted = latest_visible_ts is not None and int(latest_visible_ts) >= int(required_ts)
                            if admitted:
                                wait_state = "admitted"
                                break
                            if self.tick_queue is None:
                                wait_state = "exhausted"
                                wait_exhausted = True
                                procedural_stop = "no_tick_queue"
                                break
                            drained_any = False
                            while True:
                                head = getattr(self.tick_queue, "_queue", None)
                                head_item = head[0] if head else None
                                head_ts = head_item[0] if head_item else None
                                if head_ts is None or int(head_ts) > int(timestamp):
                                    break
                                raw = await self.tick_queue.get()
                                if isinstance(raw, tuple) and len(raw) == 2:
                                    _ts, item = raw
                                elif isinstance(raw, tuple) and len(raw) == 3:
                                    _ts, _seq, item = raw
                                else:
                                    item = raw
                                assert isinstance(item, IngestionTick)
                                self.engine.ingest_tick(item)
                                drained_ticks += 1
                                drained_any = True
                            if not drained_any:
                                head = getattr(self.tick_queue, "_queue", None)
                                head_item = head[0] if head else None
                                head_ts = head_item[0] if head_item else None
                                buffered_next_tick_ts = (
                                    self._extract_tick_timestamp(self._next_tick) if self._next_tick is not None else None
                                )
                                if buffered_next_tick_ts is not None and int(buffered_next_tick_ts) > int(timestamp):
                                    procedural_stop = "future_head"
                                elif head_ts is not None and int(head_ts) > int(timestamp):
                                    procedural_stop = "future_head"
                                else:
                                    procedural_stop = "no_current_items"
                                ingestion_done = self._ingestion_tasks is None or all(
                                    t.done() for t in self._ingestion_tasks
                                )
                                if ingestion_done:
                                    wait_state = "exhausted"
                                    wait_exhausted = True
                                    break
                                idle_wait_rounds += 1
                                total_wait_rounds += 1
                                if idle_wait_rounds >= int(OPTION_CHAIN_IDLE_WAIT_ROUNDS):
                                    wait_state = "exhausted"
                                    wait_exhausted = True
                                    break
                                await asyncio.sleep(float(OPTION_CHAIN_IDLE_WAIT_SLEEP_S))
                                continue
                            idle_wait_rounds = 0

                        latest_visible_ts = (
                            option_chain_handler.last_timestamp()
                            if callable(getattr(option_chain_handler, "last_timestamp", None))
                            else None
                        )
                        admitted = latest_visible_ts is not None and int(latest_visible_ts) >= int(required_ts)
                        buffered_next_tick_ts = (
                            self._extract_tick_timestamp(self._next_tick) if self._next_tick is not None else None
                        )
                        head = getattr(self.tick_queue, "_queue", None) if self.tick_queue is not None else None
                        head_item = head[0] if head else None
                        queue_head_ts = head_item[0] if head_item else None
                        lag_ms = int(timestamp) - int(latest_visible_ts) if latest_visible_ts is not None else None
                        reason = (
                            "option_chain_data_missing"
                            if latest_visible_ts is None
                            else "option_chain_data_lag"
                        )
                        if not admitted and procedural_stop is None:
                            if buffered_next_tick_ts is not None and int(buffered_next_tick_ts) > int(timestamp):
                                procedural_stop = "future_head"
                            elif queue_head_ts is not None and int(queue_head_ts) > int(timestamp):
                                procedural_stop = "future_head"
                            elif self.tick_queue is None:
                                procedural_stop = "no_tick_queue"
                            else:
                                procedural_stop = "no_current_items"
                        option_chain_ctx = {
                            "domain": "option_chain",
                            "symbol": option_chain_symbol,
                            "handler": option_chain_handler,
                            "admitted": bool(admitted),
                            "wait_state": "admitted" if admitted else wait_state,
                            "wait_exhausted": bool(wait_exhausted) if not admitted else False,
                            "wait_rounds": int(total_wait_rounds),
                            "latest_visible_ts": int(latest_visible_ts) if latest_visible_ts is not None else None,
                            "required_ts": int(required_ts),
                            "allowed_lag_ms": int(allowed_lag_ms),
                            "candidate_horizon_ms": int(getattr(self.spec, "interval_ms", 0) or 0),
                            "reason": None if admitted else reason,
                            "procedural_stop": None if admitted else procedural_stop,
                            "queue_head_ts": int(queue_head_ts) if queue_head_ts is not None else None,
                            "buffered_next_tick_ts": int(buffered_next_tick_ts) if buffered_next_tick_ts is not None else None,
                        }
                        if not admitted:
                            warn_key = throttle_key(
                                "backtest.option_chain.not_admitted",
                                option_chain_symbol,
                                reason,
                                procedural_stop,
                            )
                            if log_throttle(warn_key, float(OPTION_CHAIN_NOT_ADMITTED_LOG_THROTTLE_S)):
                                log_warn(
                                    self._logger,
                                    "backtest.option_chain.not_admitted",
                                    step_ts=int(timestamp),
                                    domain="option_chain",
                                    symbol=option_chain_symbol,
                                    latest_visible_ts=int(latest_visible_ts) if latest_visible_ts is not None else None,
                                    required_ts=int(required_ts),
                                    lag_ms=int(lag_ms) if lag_ms is not None else None,
                                    allowed_lag_ms=int(allowed_lag_ms),
                                    queue_head_ts=int(queue_head_ts) if queue_head_ts is not None else None,
                                    buffered_next_tick_ts=int(buffered_next_tick_ts) if buffered_next_tick_ts is not None else None,
                                    reason=reason,
                                    wait_state=wait_state,
                                    wait_exhausted=bool(wait_exhausted),
                                    wait_rounds=int(total_wait_rounds),
                                    procedural_stop=procedural_stop,
                                )
                    setattr(self.engine, "_step_option_chain_ctx", option_chain_ctx)
                log_debug(
                    self._logger,
                    "driver.ingest",
                    timestamp=timestamp,
                    drained_ticks_count=drained_ticks,
                )

                # ---- step ----
                self.guard.enter(RuntimePhase.STEP)

                result = self.engine.step(ts=timestamp)

                await asyncio.sleep(0)  # let ingestion tasks run

                if not isinstance(result, EngineSnapshot):
                    raise TypeError(f"engine.step() must return EngineSnapshot, got {type(result).__name__}")
                self._snapshots.append(result)
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
            self.guard.enter(RuntimePhase.FINISH)
            log_info(self._logger, "driver.phase.finish", timestamp=self.end_ts)
        except asyncio.CancelledError:
            self._shutdown_components()
            raise
        except Exception as exc:
            self._handle_fatal(exc)
        finally:
            await self._cancel_background_tasks()
