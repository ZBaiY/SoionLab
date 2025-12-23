from __future__ import annotations

import time
from typing import Iterable, Callable

from quant_engine.runtime.driver import BaseDriver
from quant_engine.runtime.tick import Tick
from quant_engine.runtime.modes import EngineSpec
from quant_engine.strategy.engine import StrategyEngine


class RealtimeDriver(BaseDriver):
    """
    Realtime trading driver (v4).

    Semantics:
      - Engine-time advances according to EngineSpec.advance().
      - Tick ingestion is best-effort (websocket / polling).
      - Runtime loop is open-ended.
    """

    def __init__(
        self,
        *,
        engine: StrategyEngine,
        spec: EngineSpec,
        tick_source: Callable[[], Iterable[Tick]],
    ):
        super().__init__(engine=engine, spec=spec)
        self._tick_source = tick_source

    # -------------------------------------------------
    # Time progression
    # -------------------------------------------------

    def iter_timestamps(self) -> Iterable[float]:
        """
        Yield engine-time timestamps indefinitely.

        Uses wall-clock time as the lower bound and advances
        deterministically using EngineSpec.advance().
        """
        timestamp = self.spec.timestamp or time.time()
        while True:
            yield timestamp
            timestamp = self.spec.advance(timestamp)

    # -------------------------------------------------
    # Tick ingestion
    # -------------------------------------------------

    def iter_ticks(self, *, until_ts: float) -> Iterable[Tick]:
        """
        Yield all available ticks up to `until_ts`.

        The tick_source callable is expected to return
        a *non-blocking* iterable of new ticks.
        """
        for tick in self._tick_source():
            if tick.event_ts <= until_ts:
                yield tick