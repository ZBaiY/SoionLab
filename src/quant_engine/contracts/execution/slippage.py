from typing import Protocol, Dict
from .order import Order


class SlippageModel(Protocol):
    def apply(
        self,
        order: Order,
        market_data: Dict
    ) -> float:
        """
        Return final execution price including slippage impact.
        """

        """
        ✔ 输入：原 price 和 qty
        ✔ 输出：滑点调整后的价格（float）
        用途：slippage_impl.py
        """
        ...