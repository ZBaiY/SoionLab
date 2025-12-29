from typing import Protocol, List, Dict
from .order import Order, OrderSide, OrderType


class Router(Protocol):
    def route(
        self,
        orders: List[Order],
        market_data: Dict
    ) -> List[Order]:
        """
        Modify or reroute orders:
        - Maker/taker priority
        - Multi-venue splitting
        - Cancel/replace
        - Timeout logic
        """
        ...

class RouterBase(Router):
    def __init__(self, symbol: str, **kwargs):
        self.symbol = symbol

    def route(
        self,
        orders: List[Order],
        market_data: Dict,
    ) -> List[Order]:
        raise NotImplementedError("Execution router must implement route()")