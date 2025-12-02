from typing import Protocol, List
from .execution import Order

class Router(Protocol):
    def route(self, orders: List[Order]):
        ...