from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List

from quant_engine.runtime.modes import EngineMode
from quant_engine.contracts.portfolio import PortfolioState

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class EngineSnapshot:
    """
    Immutable snapshot of a single engine step.
    """
    timestamp: int  # epoch ms
    mode: EngineMode

    # --- strategy perception ---
    features: Dict[str, Any]
    model_outputs: Dict[str, Any]

    # --- decision & execution ---
    decision_score: Any
    target_position: Any
    fills: List[Dict]

    # --- market & accounting ---
    market_data: Any  # typically a per-domain snapshot map
    portfolio: PortfolioState

    def __init__(
        self,
        timestamp: int,
        mode: EngineMode,
        features: Dict[str, Any],
        model_outputs: Dict[str, Any],
        decision_score: Any,
        target_position: Any,
        fills: List[Dict],
        market_data: Any,
        portfolio: PortfolioState,
    ):
        object.__setattr__(self, "timestamp", int(timestamp))
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "features", features)
        object.__setattr__(self, "model_outputs", model_outputs)
        object.__setattr__(self, "decision_score", decision_score)
        object.__setattr__(self, "target_position", target_position)
        object.__setattr__(self, "fills", fills)
        object.__setattr__(self, "market_data", market_data)
        object.__setattr__(self, "portfolio", portfolio)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": int(self.timestamp),
            "mode": self.mode.value,
            "features": self.features,
            "model_outputs": self.model_outputs,
            "decision_score": self.decision_score,
            "target_position": self.target_position,
            "fills": self.fills,
            "market_data": self.market_data,
            "portfolio": self.portfolio.to_dict(),
        }
