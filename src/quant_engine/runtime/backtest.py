from __future__ import annotations

from typing import Iterable

from quant_engine.runtime.driver import BaseDriver
from quant_engine.runtime.tick import Tick
from quant_engine.runtime.modes import EngineSpec
from quant_engine.strategy.engine import StrategyEngine


class BacktestDriver(BaseDriver):
    """
    Deterministic backtest driver (v4).

    Responsibilities:
      - Define a finite, deterministic engine-time timeline.
      - Replay historical ticks in event-time order.
      - Delegate all orchestration to BaseDriver.run().
    """

    def __init__(
        self,
        *,
        engine: StrategyEngine,
        spec: EngineSpec,
        start_ts: float,
        end_ts: float,
        tick_iter: Iterable[Tick],
    ):
        super().__init__(engine=engine, spec=spec)
        self.start_ts = float(start_ts)
        self.end_ts = float(end_ts)
        self._tick_iter = iter(tick_iter)
        self._next_tick: Tick | None = None

    # -------------------------------------------------
    # Time progression
    # -------------------------------------------------

    def iter_timestamps(self) -> Iterable[float]:
        """
        Yield deterministic engine-time timestamps.
        """
        timestamp = self.start_ts
        while timestamp <= self.end_ts:
            yield timestamp
            timestamp = self.spec.advance(timestamp)

    # -------------------------------------------------
    # Tick ingestion
    # -------------------------------------------------

    def iter_ticks(self, *, until_ts: float) -> Iterable[Tick]:
        """
        Yield all ticks with event_ts <= until_ts.
        """
        if self._next_tick is None:
            try:
                self._next_tick = next(self._tick_iter)
            except StopIteration:
                return

        while self._next_tick is not None and self._next_tick.event_ts <= until_ts:
            yield self._next_tick
            try:
                self._next_tick = next(self._tick_iter)
            except StopIteration:
                self._next_tick = None
                return
