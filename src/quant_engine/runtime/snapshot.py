

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from quant_engine.runtime.context import RuntimeContext


@dataclass(frozen=True)
class EngineSnapshot:
    """
    Runtime-level engine snapshot.

    Semantics:
      - Represents the result of one engine.step(ts).
      - Owned by runtime / driver.
      - Used for backtest records, artifacts, and debugging.

    Non-responsibilities:
      - NOT a market data snapshot.
      - NOT a data-layer Snapshot.
      - Does NOT own handlers or caches.
    """

    context: RuntimeContext
    features: Dict[str, Any]
    model_outputs: Dict[str, Any]
    decision_score: Any
    target_position: Any
    fills: List[Any]
    market_data: Any | None = None