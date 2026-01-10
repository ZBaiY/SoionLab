from typing import Any, Protocol, Dict
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
    SCHEMA_VERSION = 2

    def __init__(self, symbol: str, **kwargs):
        self.symbol = symbol

    def apply(
        self,
        orders: list[Order],
        market_data: dict[str, Any] | None,
    ) -> list[Order]:
        raise NotImplementedError("Slippage model must implement apply()")

    def market_status(self, market_data: Any | None) -> str | None:
        if market_data is None:
            return None
        market = market_data.get_attr("market")
        status = getattr(market, "status", None)
        if status is not None:
            return str(status)
        return None

    def market_is_active(self, market_data: dict[str, Any] | None) -> bool:
        status = self.market_status(market_data)
        if status is None:
            return True
        return str(status).lower() == "open"
