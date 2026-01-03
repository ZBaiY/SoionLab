from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import copy


@dataclass(frozen=True, slots=True)
class NormalizedStrategyCfg:
    strategy_name: str
    interval: str
    interval_ms: int | None
    required_data: tuple[str, ...]
    data: dict[str, Any]
    features_user: list[dict[str, Any]]
    model: dict[str, Any] | None
    decision: dict[str, Any] | None
    risk: dict[str, Any] | None
    execution: dict[str, Any] | None
    portfolio: dict[str, Any] | None
    universe: dict[str, Any] | None = None
    symbol: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "interval": self.interval,
            "interval_ms": self.interval_ms,
            "required_data": list(self.required_data),
            "data": copy.deepcopy(self.data),
            "features_user": copy.deepcopy(self.features_user),
            "model": copy.deepcopy(self.model),
            "decision": copy.deepcopy(self.decision),
            "risk": copy.deepcopy(self.risk),
            "execution": copy.deepcopy(self.execution),
            "portfolio": copy.deepcopy(self.portfolio),
            "symbol": self.symbol,
        }
        if self.universe is not None:
            out["universe"] = copy.deepcopy(self.universe)
        return out
