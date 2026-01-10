from typing import Any, Protocol, List, Dict
from .order import Order, OrderSide, OrderType


class ExecutionPolicy(Protocol):
    def generate(
        self,
        target_position: float,
        portfolio_state: Dict,
        market_data: Dict
    ) -> List[Order]:
        ...
        """
        ✔ target_position: desired target position
        ✔ portfolio_state: portfolio snapshot
        ✔ 返回：Order 对象列表（买/卖多少）
        用途：policy 模块的 TWAP / MarketOrderPolicy / VWAP 都会实现。
        """
    
class PolicyBase(ExecutionPolicy):
    SCHEMA_VERSION = 2

    def __init__(self, symbol: str, **kwargs):
        self.symbol = symbol

    def generate(
        self,
        target_position: float,
        portfolio_state: Dict,
        market_data: dict[str, Any] | None,
    ) -> List[Order]:
        raise NotImplementedError("Execution policy must implement generate()")

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
