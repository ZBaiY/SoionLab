# execution/policy/maker_first.py
from quant_engine.contracts.execution.policy import PolicyBase
from quant_engine.contracts.execution.order import (
    Order,
    OrderSide,
    OrderType,
)
from .registry import register_policy
from quant_engine.utils.logger import get_logger, log_debug


@register_policy("MAKER-FIRST")
class MakerFirstPolicy(PolicyBase):
    def __init__(self, symbol: str, spread_threshold=0.02):
        self.symbol = symbol
        self.spread_threshold = spread_threshold
        self._logger = get_logger(__name__)

    def generate(self, target_position, portfolio_state, market_data):
        log_debug(self._logger, "MakerFirstPolicy received target_position", target_position=target_position)
        current_pos = portfolio_state.get("position", 0)
        diff = target_position - current_pos

        if diff == 0:
            return []

        side = OrderSide.BUY if diff > 0 else OrderSide.SELL
        qty = abs(diff)

        log_debug(self._logger, "MakerFirstPolicy computed diff", side=side, qty=qty)
        
        assert market_data is not None, "Market data required for MakerFirstPolicy"
        best_bid = market_data["bid"]
        best_ask = market_data["ask"]
        spread = best_ask - best_bid

        # if spread is small, use limit order (maker)
        if spread <= self.spread_threshold:
            price = best_bid if side == OrderSide.BUY else best_ask
            order_type = OrderType.LIMIT
        else:
            price = None
            order_type = OrderType.MARKET

        log_debug(self._logger, "MakerFirstPolicy generated order", side=side, qty=qty, order_type=order_type, price=price)

        return [
            Order(
                symbol=self.symbol,
                side=side,
                qty=qty,
                order_type=order_type,
                price=price,
                tag="maker_first"
            )
        ]