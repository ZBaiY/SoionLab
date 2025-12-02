from typing import Protocol

class MatchingEngine(Protocol):
    def fill(self, price: float, qty: float):
        ...