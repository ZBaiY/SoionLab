from typing import Protocol, List, Dict
from .order import Order


class ExecutionPolicy(Protocol):
    def generate(
        self,
        target_position: float,
        portfolio_state: Dict,
        market_data: Dict
    ) -> List[Order]:
        """
        ✔ target_pos：你希望达到的仓位
        ✔ current_pos：当前仓位
        ✔ 返回：Order 对象列表（买/卖多少）
        用途：policy 模块的 TWAP / MarketOrderPolicy / VWAP 都会实现。
        """
        ...