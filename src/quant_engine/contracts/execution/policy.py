from typing import Protocol, List, Dict
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
    