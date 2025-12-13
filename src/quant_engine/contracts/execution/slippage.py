from typing import Protocol, Dict
from .order import Order, OrderSide, OrderType


class SlippageModel(Protocol):
    def apply(
        self,
        orders: list[Order],
        market_data: dict
    ) -> list[Order]:
        """
        Return final execution price including slippage impact.
        OrderSide/OrderType enum ensures consistent behavior across execution layer.
        """
        ...

class SlippageBase(SlippageModel):
    def __init__(self, symbol: str, **kwargs):
        self.symbol = symbol

    def apply(
        self,
        orders: list[Order],
        market_data: dict,
    ) -> list[Order]:
        raise NotImplementedError("Slippage model must implement apply()")