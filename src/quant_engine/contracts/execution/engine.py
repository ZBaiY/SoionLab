from __future__ import annotations

from typing import Any, Protocol


class ExecutionEngineProto(Protocol):
    def execute(
        self,
        timestamp: int,
        target_position: float,
        portfolio_state: dict[str, Any],
        market_data: dict[str, Any] | None,
    ) -> list:
        ...
