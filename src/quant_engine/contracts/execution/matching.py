from typing import Protocol, Dict
from .order import Order, OrderSide, OrderType


class MatchingEngine(Protocol):
    def match(
        self,
        orders: list[Order],
        market_data: dict
    ) -> list:
        ...
        """
        Execute or simulate fill.
        Returns a fill dictionary:
        {
            "fill_price": float,
            "filled_qty": float,
            "fee": float,
            "slippage": float,
            "side": OrderSide,
            "order_type": OrderType,
            "timestamp": float,
        }
        """

class MatchingBase(MatchingEngine):
    """
    V4 Matching base class.

    Responsibilities:
        • symbol-aware (single primary symbol)
        • convert Orders → Fills
        • NO portfolio mutation (handled upstream)
        • NO risk logic
        • NO order generation

    Fill contract (dict):
        {
            "symbol": str,
            "side": OrderSide,
            "order_type": OrderType,
            "fill_price": float,
            "filled_qty": float,
            "fee": float,
            "slippage": float,
            "timestamp": float,
        }
    """

    def __init__(self, symbol: str, **kwargs):
        self.symbol = symbol

    def match(
        self,
        orders: list[Order],
        market_data: dict,
    ) -> list[Dict]:
        raise NotImplementedError("Matching engine must implement match()")
