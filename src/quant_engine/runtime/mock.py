from __future__ import annotations

from typing import Iterable

from quant_engine.runtime.driver import BaseDriver
from quant_engine.runtime.tick import Tick
from quant_engine.runtime.modes import EngineSpec
from quant_engine.strategy.engine import StrategyEngine


class MockDriver(BaseDriver):
    """
    Synthetic runtime driver (v4).

    Intended for:
      - unit / integration tests
      - debugging runtime semantics
      - synthetic or adversarial tick streams

    Guarantees:
      - Deterministic time progression
      - Fully reproducible execution
    """

    def __init__(
        self,
        *,
        engine: StrategyEngine,
        spec: EngineSpec,
        timestamps: Iterable[float],
        ticks: Iterable[Tick],
    ):
        super().__init__(engine=engine, spec=spec)
        self._timestamps = list(timestamps)
        self._ticks = sorted(ticks, key=lambda t: t.event_ts)
        self._idx = 0

    # -------------------------------------------------
    # Time progression
    # -------------------------------------------------

    def iter_timestamps(self) -> Iterable[float]:
        """
        Yield pre-defined engine-time timestamps.
        """
        for timestamp in self._timestamps:
            yield float(timestamp)

    # -------------------------------------------------
    # Tick ingestion
    # -------------------------------------------------

    def iter_ticks(self, *, until_ts: float) -> Iterable[Tick]:
        """
        Yield all ticks with event_ts <= until_ts.
        """
        while self._idx < len(self._ticks):
            tick = self._ticks[self._idx]
            if tick.event_ts <= until_ts:
                self._idx += 1
                yield tick
            else:
                break
