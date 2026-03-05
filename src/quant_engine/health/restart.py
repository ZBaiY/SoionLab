from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Callable, Coroutine
from typing import Any

from quant_engine.health.events import ActionKind
from quant_engine.health.manager import HealthManager
from quant_engine.utils.logger import log_exception, log_info


class SourceRestartManager:
    def __init__(self, health: HealthManager, logger) -> None:
        self._health = health
        self._logger = logger
        self._pending: dict[tuple[str, str], asyncio.Task[None]] = {}
        self._lock = threading.Lock()

    def schedule_restart(
        self,
        domain: str,
        symbol: str,
        factory: Callable[[], Coroutine[Any, Any, Any] | asyncio.Task[Any]],
        stop_event: threading.Event,
    ) -> None:
        key = (str(domain), str(symbol))
        with self._lock:
            pending = self._pending.get(key)
            if pending is not None and not pending.done():
                # Invariant: at most one restart task is active per (domain, symbol).
                return
        delay_ms = self._health.restart_delay_ms(domain, symbol)
        task = asyncio.create_task(
            self._restart_after_delay(
                domain=domain,
                symbol=symbol,
                delay_ms=delay_ms,
                factory=factory,
                stop_event=stop_event,
            ),
            name=f"health.restart:{domain}:{symbol}",
        )
        with self._lock:
            self._pending[key] = task

        def _done_callback(t: asyncio.Task[None]) -> None:
            with self._lock:
                current = self._pending.get(key)
                if current is t:
                    self._pending.pop(key, None)
            if t.cancelled():
                return
            try:
                exc = t.exception()
            except asyncio.CancelledError:
                return
            if exc is None:
                return
            log_exception(
                self._logger,
                "health.restart.task_failed",
                domain=domain,
                symbol=symbol,
                err_type=type(exc).__name__,
                err=str(exc),
            )

        task.add_done_callback(_done_callback)

    async def _restart_after_delay(
        self,
        domain: str,
        symbol: str,
        delay_ms: int,
        factory: Callable[[], Coroutine[Any, Any, Any] | asyncio.Task[Any]],
        stop_event: threading.Event,
    ) -> None:
        await asyncio.sleep(max(0.0, float(delay_ms) / 1000.0))
        if stop_event.is_set():
            return
        action = self._health.report_recovery(domain, symbol)
        if action.kind == ActionKind.HALT:
            stop_event.set()
            return
        log_info(
            self._logger,
            "health.restart.scheduled",
            domain=domain,
            symbol=symbol,
            delay_ms=int(delay_ms),
        )
        started = factory()
        if isinstance(started, asyncio.Task):
            return
        if inspect.isawaitable(started):
            # Invariant: restart factory awaitables must execute under manager-owned restart task — enforced here to prevent unowned fire-and-forget failures
            started = await started
            if inspect.isawaitable(started):
                await started

    def cancel_all(self) -> None:
        # Invariant: manager-owned restart delay tasks must be cancelled on shutdown — enforced here to prevent pending-task retention
        with self._lock:
            pending = list(self._pending.values())
            self._pending.clear()
        for task in pending:
            task.cancel()
