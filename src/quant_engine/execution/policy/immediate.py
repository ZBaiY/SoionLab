from quant_engine.contracts.execution.policy import PolicyBase
from quant_engine.contracts.execution.order import Order, OrderSide, OrderType
from .registry import register_policy
from quant_engine.utils.logger import get_logger, log_debug


@register_policy("IMMEDIATE")
class ImmediatePolicy(PolicyBase):
    def __init__(self, symbol: str):
        self.symbol = symbol
        self._logger = get_logger(__name__)

    def generate(self, target_position, portfolio_state, market_data):
        log_debug(self._logger, "ImmediatePolicy received target_position", target_position=target_position)
        current_pos = portfolio_state.get("position", 0)
        diff = target_position - current_pos
        
        if diff == 0:
            return []

        side = "BUY" if diff > 0 else "SELL"
        qty = abs(diff)

        log_debug(self._logger, "ImmediatePolicy generated order", side=side, qty=qty)

        return [
            Order(
                symbol=self.symbol,
                side=OrderSide.BUY if diff > 0 else OrderSide.SELL,
                qty=qty,
                order_type=OrderType.MARKET,
                price=None,
                timestamp=None,
                tag="immediate",
                extra={
                    "time_in_force": OrderType.IOC.value
                }
            )
        ]
