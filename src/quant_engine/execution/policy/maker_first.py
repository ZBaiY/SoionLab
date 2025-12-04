# execution/policy/maker_first.py
from quant_engine.contracts.execution.policy import ExecutionPolicy
from quant_engine.contracts.execution.order import Order
from .registry import register_policy


@register_policy("MAKER_FIRST")
class MakerFirstPolicy(ExecutionPolicy):
    def __init__(self, spread_threshold=0.02):
        self.spread_threshold = spread_threshold

    def generate(self, target_position, portfolio_state, market_data):
        current_pos = portfolio_state.get("position", 0)
        diff = target_position - current_pos

        if diff == 0:
            return []

        side = "BUY" if diff > 0 else "SELL"
        qty = abs(diff)

        best_bid = market_data["bid"]
        best_ask = market_data["ask"]
        spread = best_ask - best_bid

        # if spread is small, use limit order (maker)
        if spread <= self.spread_threshold:
            price = best_bid if side == "BUY" else best_ask
            order_type = "LIMIT"
        else:
            price = None
            order_type = "MARKET"

        return [
            Order(
                side=side,
                qty=qty,
                order_type=order_type,
                price=price,
                tag="maker_first"
            )
        ]