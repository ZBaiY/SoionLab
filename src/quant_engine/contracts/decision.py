from typing import Protocol

class DecisionProto(Protocol):
    def decide(self, score: float) -> float:
        ...