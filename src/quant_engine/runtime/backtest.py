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

OhlcvHandlerSpec = tuple[Any, int, str]
NotReadyOhlcvSpec = tuple[Any, int, str, int | None]


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

    def _normalize_queue_item(self, raw: Any) -> tuple[int, Any]:
        if isinstance(raw, tuple) and len(raw) == 2:
            ts, item = raw
            return ensure_epoch_ms(ts), item
        if isinstance(raw, tuple) and len(raw) == 3:
            ts, _seq, item = raw
            return ensure_epoch_ms(ts), item
        item = raw
        return self._extract_tick_timestamp(item), item

    def _peek_queue_head(self) -> tuple[int | None, Any | None]:
        if self.tick_queue is None:
            return None, None
        head = getattr(self.tick_queue, "_queue", None)
        head_item = head[0] if head else None
        if head_item is None:
            return None, None
        return self._normalize_queue_item(head_item)

    def _last_handler_timestamp(self, handler: Any) -> int | None:
        if not callable(getattr(handler, "last_timestamp", None)):
            return None
        ts = handler.last_timestamp()
        return int(ts) if ts is not None else None

    def _buffered_next_tick_ts(self) -> int | None:
        if self._next_tick is None:
            return None
        return self._extract_tick_timestamp(self._next_tick)

    def _has_finished_ingestion_tasks(self) -> bool:
        return self._ingestion_tasks is not None and all(t.done() for t in self._ingestion_tasks)

    def _is_ingestion_exhausted(self) -> bool:
        return self._ingestion_tasks is None or all(t.done() for t in self._ingestion_tasks)

    def _resolve_driver_gating(self) -> tuple[list[OhlcvHandlerSpec], Any | None, Any | None, int | None]:
        required_ohlcv_handlers: list[OhlcvHandlerSpec] = []
        option_chain_handler = None
        option_chain_symbol = None
        option_chain_interval_ms: int | None = None
        if self.spec.mode not in (EngineMode.BACKTEST, EngineMode.SAMPLE):
            return required_ohlcv_handlers, option_chain_handler, option_chain_symbol, option_chain_interval_ms

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

        return required_ohlcv_handlers, option_chain_handler, option_chain_symbol, option_chain_interval_ms

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

            ts, item = self._normalize_queue_item(raw)

            if ts <= until:
                yield item
                n += 1
                if self._drain_yield_every > 0 and (n % self._drain_yield_every) == 0:
                    await asyncio.sleep(0)  # don’t hog the loop on huge queues
            else:
                self._next_tick = item
                return

    async def _drain_pending_queue_ticks(self, *, timestamp: int) -> int:
        drained = 0
        while True:
            head_ts, _head_item = self._peek_queue_head()
            if head_ts is None or int(head_ts) > int(timestamp):
                return drained
            assert self.tick_queue is not None
            raw = await self.tick_queue.get()
            _ts, item = self._normalize_queue_item(raw)
            assert isinstance(item, IngestionTick)
            self.engine.ingest_tick(item)
            drained += 1

    def _collect_not_ready_ohlcv(
        self,
        *,
        timestamp: int,
        required_ohlcv_handlers: list[OhlcvHandlerSpec],
        last_tick_ts_by_key: dict[str, Any],
    ) -> tuple[list[NotReadyOhlcvSpec], dict[str, tuple[Any, int, int | None]]]:
        not_ready: list[NotReadyOhlcvSpec] = []
        not_ready_by_key: dict[str, tuple[Any, int, int | None]] = {}
        for handler, interval_ms, key in required_ohlcv_handlers:
            need_ts = int(visible_end_ts(timestamp, interval_ms))
            watermark = last_tick_ts_by_key.get(key)
            watermark_int = int(watermark) if watermark is not None else self._last_handler_timestamp(handler)
            if watermark_int is None or watermark_int < need_ts:
                not_ready.append((handler, need_ts, key, watermark_int))
                not_ready_by_key[key] = (handler, need_ts, watermark_int)
        return not_ready, not_ready_by_key

    def _raise_if_ohlcv_data_missing(
        self,
        *,
        timestamp: int,
        not_ready: list[NotReadyOhlcvSpec],
    ) -> None:
        if not self._has_finished_ingestion_tasks():
            return
        details = [
            f"{getattr(handler, 'symbol', None)}:{need_ts}:{watermark}"
            for handler, need_ts, _key, watermark in not_ready
        ]
        raise RuntimeError(f"backtest.missing_data: step_ts={timestamp} missing={details}")

    def _raise_if_future_required_tick_blocks(
        self,
        *,
        timestamp: int,
        required_keys: set[str],
        not_ready_by_key: dict[str, tuple[Any, int, int | None]],
        last_tick_ts_by_key: dict[str, Any],
    ) -> None:
        head_ts, head_item = self._peek_queue_head()
        head_key = None
        if isinstance(head_item, IngestionTick):
            head_key = format_tick_key(head_item.domain, head_item.symbol, getattr(head_item, "source_id", None))
        if (
            head_ts is None
            or int(head_ts) <= int(timestamp)
            or head_key not in required_keys
            or head_key not in not_ready_by_key
        ):
            return
        handler, need_ts, _wm = not_ready_by_key[head_key]
        watermark_engine = last_tick_ts_by_key.get(head_key)
        watermark_handler = self._last_handler_timestamp(handler)
        raise RuntimeError(
            "backtest.missing_data"
            f": step_ts={timestamp}"
            f" key={head_key}"
            f" need_ts={need_ts}"
            f" head_ts={head_ts}"
            f" watermark_from_engine={watermark_engine}"
            f" watermark_from_handler={watermark_handler}"
        )

    def _log_closed_bar_not_ready(
        self,
        *,
        timestamp: int,
        required_ohlcv_handlers: list[OhlcvHandlerSpec],
    ) -> None:
        for handler, interval_ms, _key in required_ohlcv_handlers:
            need_ts = int(visible_end_ts(timestamp, interval_ms))
            actual_last_ts = self._last_handler_timestamp(handler)
            if actual_last_ts is not None and int(actual_last_ts) >= int(need_ts):
                continue
            symbol = getattr(handler, "symbol", None)
            handler_type = handler.__class__.__name__
            interval_val = getattr(handler, "interval", None) or getattr(handler, "interval_ms", None)
            key = throttle_key("backtest.closed_bar.not_ready", handler_type, symbol, interval_val)
            lag_ms = int(need_ts) - int(actual_last_ts) if actual_last_ts is not None else None
            normalized_interval_ms = int(interval_ms) if isinstance(interval_ms, int) else None
            actionable = (
                lag_ms is not None
                and normalized_interval_ms is not None
                and lag_ms >= 3 * int(normalized_interval_ms)
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
                    interval_ms=normalized_interval_ms,
                )

    async def _gate_required_ohlcv(
        self,
        *,
        timestamp: int,
        required_ohlcv_handlers: list[OhlcvHandlerSpec],
    ) -> int:
        if not required_ohlcv_handlers or self.tick_queue is None:
            return 0
        drained_ticks = 0
        required_keys = {key for _handler, _interval_ms, key in required_ohlcv_handlers}
        while True:
            last_tick_ts_by_key = getattr(self.engine, "_last_tick_ts_by_key", {}) or {}
            not_ready, not_ready_by_key = self._collect_not_ready_ohlcv(
                timestamp=timestamp,
                required_ohlcv_handlers=required_ohlcv_handlers,
                last_tick_ts_by_key=last_tick_ts_by_key,
            )
            if not not_ready:
                break
            self._raise_if_ohlcv_data_missing(timestamp=timestamp, not_ready=not_ready)
            drained_now = await self._drain_pending_queue_ticks(timestamp=timestamp)
            drained_ticks += drained_now
            if drained_now > 0:
                continue
            self._raise_if_future_required_tick_blocks(
                timestamp=timestamp,
                required_keys=required_keys,
                not_ready_by_key=not_ready_by_key,
                last_tick_ts_by_key=last_tick_ts_by_key,
            )
            await asyncio.sleep(0)
        self._log_closed_bar_not_ready(timestamp=timestamp, required_ohlcv_handlers=required_ohlcv_handlers)
        return drained_ticks

    def _resolve_procedural_stop(self, *, timestamp: int, queue_head_ts: int | None) -> str:
        buffered_next_tick_ts = self._buffered_next_tick_ts()
        if buffered_next_tick_ts is not None and int(buffered_next_tick_ts) > int(timestamp):
            return "future_head"
        if queue_head_ts is not None and int(queue_head_ts) > int(timestamp):
            return "future_head"
        if self.tick_queue is None:
            return "no_tick_queue"
        return "no_current_items"

    def _log_option_chain_not_admitted(self, *, timestamp: int, ctx: dict[str, Any]) -> None:
        warn_key = throttle_key(
            "backtest.option_chain.not_admitted",
            ctx["symbol"],
            ctx["reason"],
            ctx["procedural_stop"],
        )
        if not log_throttle(warn_key, float(OPTION_CHAIN_NOT_ADMITTED_LOG_THROTTLE_S)):
            return
        lag_ms = None
        latest_visible_ts = ctx["latest_visible_ts"]
        if latest_visible_ts is not None:
            lag_ms = int(timestamp) - int(latest_visible_ts)
        log_warn(
            self._logger,
            "backtest.option_chain.not_admitted",
            step_ts=int(timestamp),
            domain="option_chain",
            symbol=ctx["symbol"],
            latest_visible_ts=latest_visible_ts,
            required_ts=ctx["required_ts"],
            lag_ms=lag_ms,
            allowed_lag_ms=ctx["allowed_lag_ms"],
            queue_head_ts=ctx["queue_head_ts"],
            buffered_next_tick_ts=ctx["buffered_next_tick_ts"],
            reason=ctx["reason"],
            wait_state=ctx["wait_state"],
            wait_exhausted=ctx["wait_exhausted"],
            wait_rounds=ctx["wait_rounds"],
            procedural_stop=ctx["procedural_stop"],
        )

    async def _evaluate_option_chain_admission(
        self,
        *,
        timestamp: int,
        option_chain_handler: Any,
        option_chain_symbol: Any,
        option_chain_interval_ms: int,
    ) -> tuple[dict[str, Any], int]:
        drained_ticks = 0
        allowed_lag_ms = int(option_chain_interval_ms) * 2
        required_ts = int(timestamp) - int(allowed_lag_ms)
        idle_wait_rounds = 0
        total_wait_rounds = 0
        wait_state = "waiting"
        wait_exhausted = False
        procedural_stop = None

        while True:
            latest_visible_ts = self._last_handler_timestamp(option_chain_handler)
            admitted = latest_visible_ts is not None and int(latest_visible_ts) >= int(required_ts)
            if admitted:
                wait_state = "admitted"
                break
            if self.tick_queue is None:
                wait_state = "exhausted"
                wait_exhausted = True
                procedural_stop = "no_tick_queue"
                break

            drained_now = await self._drain_pending_queue_ticks(timestamp=timestamp)
            drained_ticks += drained_now
            if drained_now > 0:
                idle_wait_rounds = 0
                continue

            queue_head_ts, _head_item = self._peek_queue_head()
            procedural_stop = self._resolve_procedural_stop(timestamp=timestamp, queue_head_ts=queue_head_ts)
            if self._is_ingestion_exhausted():
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

        latest_visible_ts = self._last_handler_timestamp(option_chain_handler)
        admitted = latest_visible_ts is not None and int(latest_visible_ts) >= int(required_ts)
        queue_head_ts, _head_item = self._peek_queue_head()
        buffered_next_tick_ts = self._buffered_next_tick_ts()
        reason = None
        if not admitted:
            reason = "option_chain_data_missing" if latest_visible_ts is None else "option_chain_data_lag"
            if procedural_stop is None:
                procedural_stop = self._resolve_procedural_stop(timestamp=timestamp, queue_head_ts=queue_head_ts)

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
            "reason": reason,
            "procedural_stop": None if admitted else procedural_stop,
            "queue_head_ts": int(queue_head_ts) if queue_head_ts is not None else None,
            "buffered_next_tick_ts": int(buffered_next_tick_ts) if buffered_next_tick_ts is not None else None,
        }
        if not admitted:
            self._log_option_chain_not_admitted(timestamp=timestamp, ctx=option_chain_ctx)
        return option_chain_ctx, drained_ticks

    async def _ingest_for_step(
        self,
        *,
        timestamp: int,
        required_ohlcv_handlers: list[OhlcvHandlerSpec],
        option_chain_handler: Any | None,
        option_chain_symbol: Any | None,
        option_chain_interval_ms: int | None,
    ) -> int:
        self.guard.enter(RuntimePhase.INGEST)
        self.engine.align_to(timestamp)

        drained_ticks = 0
        async for tick in self.drain_ticks(until_timestamp=timestamp):
            self.engine.ingest_tick(tick)
            drained_ticks += 1

        option_chain_ctx = None
        if self.spec.mode in (EngineMode.BACKTEST, EngineMode.SAMPLE):
            drained_ticks += await self._gate_required_ohlcv(
                timestamp=timestamp,
                required_ohlcv_handlers=required_ohlcv_handlers,
            )
            if option_chain_handler is not None and option_chain_interval_ms is not None:
                option_chain_ctx, option_drained_ticks = await self._evaluate_option_chain_admission(
                    timestamp=timestamp,
                    option_chain_handler=option_chain_handler,
                    option_chain_symbol=option_chain_symbol,
                    option_chain_interval_ms=option_chain_interval_ms,
                )
                drained_ticks += option_drained_ticks
            setattr(self.engine, "_step_option_chain_ctx", option_chain_ctx)

        log_debug(
            self._logger,
            "driver.ingest",
            timestamp=timestamp,
            drained_ticks_count=drained_ticks,
        )
        return drained_ticks

    async def _run_step(
        self,
        *,
        timestamp: int,
        required_ohlcv_handlers: list[OhlcvHandlerSpec],
        option_chain_handler: Any | None,
        option_chain_symbol: Any | None,
        option_chain_interval_ms: int | None,
    ) -> int:
        log_debug(self._logger, "driver.phase.step", timestamp=timestamp)
        drained_ticks = await self._ingest_for_step(
            timestamp=timestamp,
            required_ohlcv_handlers=required_ohlcv_handlers,
            option_chain_handler=option_chain_handler,
            option_chain_symbol=option_chain_symbol,
            option_chain_interval_ms=option_chain_interval_ms,
        )

        self.guard.enter(RuntimePhase.STEP)
        result = self.engine.step(ts=timestamp)
        await asyncio.sleep(0)  # let ingestion tasks run

        if not isinstance(result, EngineSnapshot):
            raise TypeError(f"engine.step() must return EngineSnapshot, got {type(result).__name__}")
        self._snapshots.append(result)
        return drained_ticks

    async def _load_history(self) -> None:
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

    async def _warmup_features(self) -> None:
        self.guard.enter(RuntimePhase.WARMUP)
        log_info(self._logger, "driver.phase.warmup", timestamp=self.start_ts)
        await to_thread_limited(
            self.engine.warmup_features,
            anchor_ts=self.start_ts,
            logger=self._logger,
            context={"driver": self.__class__.__name__},
            op="warmup_features",
        )

    async def run(self) -> None:
        self._install_loop_exception_handler()
        try:
            await self._load_history()
            await self._warmup_features()
            (
                required_ohlcv_handlers,
                option_chain_handler,
                option_chain_symbol,
                option_chain_interval_ms,
            ) = self._resolve_driver_gating()

            step_count = 0
            async for ts in self.iter_timestamps():
                if self.stop_event.is_set():
                    break
                timestamp = int(ts)
                drained_ticks = await self._run_step(
                    timestamp=timestamp,
                    required_ohlcv_handlers=required_ohlcv_handlers,
                    option_chain_handler=option_chain_handler,
                    option_chain_symbol=option_chain_symbol,
                    option_chain_interval_ms=option_chain_interval_ms,
                )
                step_count += 1
                if self._step_log_every > 0 and (step_count % self._step_log_every) == 0:
                    log_debug(
                        self._logger,
                        "driver.step",
                        timestamp=timestamp,
                        drained_ticks_count=drained_ticks,
                        snapshots_len=len(self._snapshots),
                    )

            self.guard.enter(RuntimePhase.FINISH)
            log_info(self._logger, "driver.phase.finish", timestamp=self.end_ts)
        except asyncio.CancelledError:
            self._shutdown_components()
            raise
        except Exception as exc:
            self._handle_fatal(exc)
        finally:
            await self._cancel_background_tasks()
