from typing import Any, Protocol, Dict
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
    SCHEMA_VERSION = 2

    def __init__(self, symbol: str, **kwargs):
        self.symbol = symbol

    def match(
        self,
        orders: list[Order],
        market_data: dict[str, Any] | None,
    ) -> list[Dict]:
        raise NotImplementedError("Matching engine must implement match()")

    def market_status(self, market_data: dict[str, Any] | None) -> str | None:
        if not isinstance(market_data, dict):
            return None
        market = market_data.get("market")
        if isinstance(market, dict):
            status = market.get("status")
            if status is not None:
                return str(status)
        return None

    def market_is_active(self, market_data: dict[str, Any] | None) -> bool:
        status = self.market_status(market_data)
        if status is None:
            return True
        return str(status).lower() == "open"
