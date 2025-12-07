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

        """
        ✔ 输入：price、qty
        ✔ 输出：你的实现可以是 fill-price、actual成交信息等
        ✔ 这个接口很自由，你保留简化也可以
        用途：matching_sim.py / matching_live.py
        """