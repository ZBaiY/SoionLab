from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, List, Optional
from quant_engine.runtime.tick import Tick
from quant_engine.runtime.lifecycle import LifecycleGuard, RuntimePhase
from quant_engine.runtime.modes import EngineSpec
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.runtime.snapshot import EngineSnapshot
from quant_engine.runtime.context import RuntimeContext


class BaseDriver(ABC):
    """
    Base class for all runtime drivers.

    Responsibilities:
      - Own runtime lifecycle ordering.
      - Own time progression (ts generation).
      - Own ingestion ordering (Tick delivery).
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
    def iter_timestamps(self) -> Iterable[float]:
        """
        Yield engine-time timestamps in strictly increasing order.
        """
        raise NotImplementedError

    @abstractmethod
    def iter_ticks(self, *, until_ts: float) -> Iterable[Tick]:
        """
        Yield all ticks with event_ts <= until_ts.

        Semantics:
          - Must be deterministic for backtest.
          - May be best-effort for realtime.
        """
        raise NotImplementedError

    # -------------------------------------------------
    # Canonical runtime loop
    # -------------------------------------------------

    def run(self) -> None:
        """
        Execute the canonical v4 runtime loop.

        PRELOAD → WARMUP → (INGEST ↔ STEP)* → FINISH
        """

        # -------- preload --------
        self.guard.enter(RuntimePhase.PRELOAD)
        self.engine.preload_data(anchor_ts=self.spec.timestamp)

        # -------- warmup --------
        self.guard.enter(RuntimePhase.WARMUP)
        self.engine.warmup_features(anchor_ts=self.spec.timestamp)

        # -------- main loop --------
        for ts in self.iter_timestamps():
            # ingest
            self.guard.enter(RuntimePhase.INGEST)
            for tick in self.iter_ticks(until_ts=ts):
                self.engine.ingest_tick(tick)

            # step
            self.guard.enter(RuntimePhase.STEP)
            self.engine.align_to(ts)

            result = self.engine.step(ts=ts)

            # Optionally collect runtime snapshot
            if isinstance(result, dict):
                snapshot = EngineSnapshot(
                    context=RuntimeContext(timestamp=ts, mode=self.spec.mode),
                    features=result.get("context", {}).get("features", {}),
                    model_outputs=result.get("context", {}).get("models", {}),
                    decision_score=result.get("decision_score"),
                    target_position=result.get("target_position"),
                    fills=result.get("fills", []),
                    market_data=result.get("market_data"),
                )
                self._snapshots.append(snapshot)

        # -------- finish --------
        self.guard.enter(RuntimePhase.FINISH)
