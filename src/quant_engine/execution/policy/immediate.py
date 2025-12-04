# execution/policy/immediate.py
from quant_engine.contracts.execution.policy import ExecutionPolicy
from quant_engine.contracts.execution.order import Order
from .registry import register_policy


@register_policy("IMMEDIATE")
class ImmediatePolicy(ExecutionPolicy):
    def __init__(self):
        pass

    def generate(self, target_position, portfolio_state, market_data):
        current_pos = portfolio_state.get("position", 0)
        diff = target_position - current_pos
        
        if diff == 0:
            return []

        side = "BUY" if diff > 0 else "SELL"
        qty = abs(diff)

        return [
            Order(
                side=side,
                qty=qty,
                order_type="MARKET",
                price=None,
                tag="immediate"
            )
        ]