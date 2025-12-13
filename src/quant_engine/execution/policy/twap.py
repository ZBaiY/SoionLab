from quant_engine.contracts.execution.policy import PolicyBase
from quant_engine.contracts.execution.order import (
    Order,
    OrderSide,
    OrderType,
)
from .registry import register_policy
from quant_engine.utils.logger import get_logger, log_debug


@register_policy("TWAP")
class TWAPPolicy(PolicyBase):
    def __init__(self, symbol: str,slices=5):
        self.symbol = symbol
        self.slices = slices
        self._logger = get_logger(__name__)

    def generate(self, target_position, portfolio_state, market_data):
        log_debug(self._logger, "TWAPPolicy received target_position", target_position=target_position, slices=self.slices)
        current_pos = portfolio_state.get("position", 0)
        diff = target_position - current_pos
        if diff == 0:
            return []

        side = OrderSide.BUY if diff > 0 else OrderSide.SELL
        qty_total = abs(diff)
        qty_each = qty_total / self.slices
        log_debug(self._logger, "TWAPPolicy computed slice parameters", side=side, qty_total=qty_total, qty_each=qty_each)

        orders = []
        for i in range(self.slices):
            log_debug(self._logger, "TWAPPolicy generated slice order", index=i, side=side, qty=qty_each)
            orders.append(
                Order(
                    symbol=self.symbol,
                    side=side,
                    qty=qty_each,
                    order_type=OrderType.MARKET,
                    price=None,
                    tag=f"twap_{i}"
                )
            )
        return orders