from typing import Protocol, List, Dict
from .order import Order


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
        """
        ✔ 输入：订单列表
        ✔ 输出：无（由实现决定如何下单/发送/传递）
        用途：router_impl.py
        """
        ...