from typing import Any, Protocol, List, Dict
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
    SCHEMA_VERSION = 2

    def __init__(self, symbol: str, **kwargs):
        self.symbol = symbol

    def route(
        self,
        orders: List[Order],
        market_data: dict[str, Any] | None,
    ) -> List[Order]:
        raise NotImplementedError("Execution router must implement route()")

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
