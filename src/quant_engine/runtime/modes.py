from __future__ import annotations

from datetime import tzinfo
from enum import Enum
from dataclasses import dataclass
from typing import Any

from quant_engine.utils.timer import advance_ts, adv_ts


class EngineMode(Enum):
    """
    Runtime execution mode.

    This enum is used by Driver / Runtime for logging, guards,
    and artifact routing. It must NOT be used to branch strategy logic.
    """

    REALTIME = "realtime"
    BACKTEST = "backtest"
    MOCK = "mock"


@dataclass(frozen=True)
class EngineSpec:
    """
    Strategy observation semantics.

    EngineSpec describes *how the strategy observes time*,
    not how data is ingested.

    Responsibilities:
      - Define the strategy-level observation interval.
      - Provide a deterministic clock advancement rule via `advance(ts)`.
    """

    mode: EngineMode
    interval: str          # e.g. "1m", "5m"
    interval_seconds: float  # e.g. 60.0, 300.0
    symbol: str
    timestamp: float | None = None
    universe: dict[str, Any] | None = None
    timezone: tzinfo | None = None ## optional timezone for multi-timezone strategies in the future

    def advance(self, ts: float) -> float:
        """
        Deterministically advance the strategy observation clock.
        """
        return adv_ts(ts, self.interval_seconds)
