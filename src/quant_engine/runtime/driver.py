from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator, Iterable, List, Optional
import threading
import traceback

from quant_engine.exceptions.core import FatalError
from quant_engine.runtime.lifecycle import LifecycleGuard, RuntimePhase
from quant_engine.runtime.modes import EngineSpec
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.runtime.snapshot import EngineSnapshot
from quant_engine.utils.guards import format_exc, join_threads
from quant_engine.utils.logger import get_logger, log_error


class BaseDriver(ABC):
    """
    Base class for all runtime drivers.

    Responsibilities:
      - Own runtime lifecycle ordering.
      - Own time progression only (no ingestion).
      - Never own strategy logic.
    """

    def __init__(
        self,
        *,
        engine: StrategyEngine,
        spec: EngineSpec,
        stop_event: threading.Event | None = None,
        shutdown_threads: list[threading.Thread] | None = None,
    ):
        self.engine = engine
        self.spec = spec
        self.guard = LifecycleGuard()
        self._stop_event = stop_event or threading.Event()
        self._shutdown_threads = shutdown_threads or []
        self._alerted = False
        self._logger = get_logger(self.__class__.__name__)
        try:
            setattr(self.engine, "stop_event", self._stop_event)
        except Exception:
            pass

        # Runtime-owned snapshots (optional)
        self._snapshots: List[EngineSnapshot] = []

    @property
    def snapshots(self) -> List[EngineSnapshot]:
        """
        Collected EngineSnapshot objects produced during runtime.

        Semantics:
          - Owned by Driver.
          - Empty if snapshots are not collected.
          - Intended for backtest / debug / artifact layers.
        """
        return self._snapshots

    @property
    def stop_event(self) -> threading.Event:
        return self._stop_event

    # -------------------------------------------------
    # Hooks for subclasses
    # -------------------------------------------------

    @abstractmethod
    async def iter_timestamps(self) -> AsyncIterator[int]:
        """
        Yield engine-time timestamps (epoch ms int) in strictly increasing order.

        Must be implemented as an async generator.
        """
        raise NotImplementedError

    # -------------------------------------------------
    # Canonical runtime loop
    # -------------------------------------------------

    async def run(self) -> None:
        """
        Execute the canonical v4 runtime loop.

        PRELOAD → WARMUP → STEP* → FINISH
        """
        raise NotImplementedError
        # -------- preload --------

    def _shutdown_components(self) -> None:
        self._stop_event.set()

        for obj in self._iter_shutdown_objects():
            for method in ("close", "shutdown", "stop"):
                fn = getattr(obj, method, None)
                if callable(fn):
                    try:
                        fn()
                    except Exception:
                        pass

        if self._shutdown_threads:
            join_threads(self._shutdown_threads, timeout_s=2.0)

    def _iter_shutdown_objects(self) -> Iterable[object]:
        yield self.engine
        for hmap in (
            self.engine.ohlcv_handlers,
            self.engine.orderbook_handlers,
            self.engine.option_chain_handlers,
            self.engine.iv_surface_handlers,
            self.engine.sentiment_handlers,
            self.engine.trades_handlers,
            self.engine.option_trades_handlers,
        ):
            for h in hmap.values():
                yield h
        for obj in (
            self.engine.feature_extractor,
            self.engine.models,
            self.engine.decision,
            self.engine.risk_manager,
            self.engine.execution_engine,
            self.engine.portfolio,
        ):
            yield obj

    def _alert_once(self, exc: BaseException) -> None:
        if self._alerted:
            return
        self._alerted = True
        summary = format_exc(exc)
        stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        log_error(
            self._logger,
            "runtime.fatal_error",
            err_type=type(exc).__name__,
            err=summary,
            stack=stack,
        )

    def _handle_fatal(self, exc: BaseException) -> None:
        self._shutdown_components()
        self._alert_once(exc)
        if isinstance(exc, FatalError):
            raise
        raise FatalError(str(exc)) from exc
