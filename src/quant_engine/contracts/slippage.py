from typing import Protocol

class SlippageModel(Protocol):
    def apply(self, price: float, qty: float) -> float:
        ...