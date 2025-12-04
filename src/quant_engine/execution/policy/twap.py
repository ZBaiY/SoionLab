# execution/policy/twap.py
from quant_engine.contracts.execution.policy import ExecutionPolicy
from quant_engine.contracts.execution.order import Order
from .registry import register_policy


@register_policy("TWAP")
class TWAPPolicy(ExecutionPolicy):
    def __init__(self, slices=5):
        self.slices = slices

    def generate(self, target_position, portfolio_state, market_data):
        current_pos = portfolio_state.get("position", 0)
        diff = target_position - current_pos
        if diff == 0:
            return []

        side = "BUY" if diff > 0 else "SELL"
        qty_total = abs(diff)
        qty_each = qty_total / self.slices

        orders = []
        for i in range(self.slices):
            orders.append(
                Order(
                    side=side,
                    qty=qty_each,
                    order_type="MARKET",
                    price=None,
                    tag=f"twap_{i}"
                )
            )
        return orders