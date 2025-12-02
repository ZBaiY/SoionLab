from typing import Protocol

class RiskProto(Protocol):
    def size(self, intent: float, volatility: float = 1.0) -> float:
        ...