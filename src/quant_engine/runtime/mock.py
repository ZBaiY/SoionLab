from __future__ import annotations

from typing import Iterable, AsyncIterator
import threading

from quant_engine.runtime.driver import BaseDriver
from ingestion.contracts.tick import IngestionTick
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
        timestamps: Iterable[int],
        ticks: Iterable[IngestionTick],
        stop_event: threading.Event | None = None,
    ):
        super().__init__(engine=engine, spec=spec, stop_event=stop_event)
        self._timestamps = [int(t) for t in timestamps]
        self._ticks = sorted(ticks, key=lambda t: t.data_ts)
        self._idx = 0

    # -------------------------------------------------
    # Time progression
    # -------------------------------------------------

    async def iter_timestamps(self) -> AsyncIterator[int]:
        """Yield pre-defined engine-time timestamps (epoch ms int)."""
        for timestamp in self._timestamps:
            yield int(timestamp)

    # -------------------------------------------------
    # Tick ingestion
    # -------------------------------------------------

    def iter_ticks(self, *, until_ts: int) -> Iterable[IngestionTick]:
        """
        Yield all ticks with data_ts <= until_ts (epoch ms int).
        """
        while self._idx < len(self._ticks):
            tick = self._ticks[self._idx]
            if int(tick.data_ts) <= int(until_ts):
                self._idx += 1
                yield tick
            else:
                break
