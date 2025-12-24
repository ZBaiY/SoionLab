from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator, Iterable, List, Optional
from quant_engine.runtime.lifecycle import LifecycleGuard, RuntimePhase
from quant_engine.runtime.modes import EngineSpec
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.runtime.snapshot import EngineSnapshot


class BaseDriver(ABC):
    """
    Base class for all runtime drivers.

    Responsibilities:
      - Own runtime lifecycle ordering.
      - Own time progression only (no ingestion).
      - Never own strategy logic.
    """

    def __init__(self, *, engine: StrategyEngine, spec: EngineSpec):
        self.engine = engine
        self.spec = spec
        self.guard = LifecycleGuard()

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

    # -------------------------------------------------
    # Hooks for subclasses
    # -------------------------------------------------

    @abstractmethod
    async def iter_timestamps(self) -> AsyncIterator[float]:
        """
        Yield engine-time timestamps in strictly increasing order.

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
    