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

        """
        ✔ 输入：原 price 和 qty
        ✔ 输出：滑点调整后的价格（float）
        用途：slippage_impl.py
        """
        ...