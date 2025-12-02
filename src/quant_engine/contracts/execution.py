from typing import Protocol, List
from dataclasses import dataclass

@dataclass
class Order:
    side: str
    qty: float

class ExecutionPolicy(Protocol):
    def generate_orders(self, target_pos: float, current_pos: float) -> List[Order]:
        ...