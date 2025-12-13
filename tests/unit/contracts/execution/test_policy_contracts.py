import pytest
from typing import Dict, Any, List

from quant_engine.contracts.execution.order import Order, OrderSide, OrderType
from quant_engine.execution.policy.registry import register_policy
from quant_engine.contracts.execution.policy import PolicyBase


@register_policy("DUMMY_POLICY")
class DummyPolicy(PolicyBase):
    """Generate one MARKET order proportional to target_position."""

    def generate(
        self,
        target_position: float,
        portfolio_state: Dict[str, Any],
        market_data: Dict[str, Any] | None,
    ) -> List[Order]:
        if target_position == 0:
            return []

        side = OrderSide.BUY if target_position > 0 else OrderSide.SELL
        return [
            Order(
                symbol=self.symbol,
                side=side,
                qty=abs(target_position),
                order_type=OrderType.MARKET,
                price=None,
            )
        ]


def test_policy_generates_order():
    policy = DummyPolicy(symbol="BTCUSDT")

    orders = policy.generate(
        target_position=1.5,
        portfolio_state={},
        market_data=None,
    )

    assert len(orders) == 1
    o = orders[0]
    assert o.symbol == "BTCUSDT"
    assert o.side == OrderSide.BUY
    assert o.qty == 1.5
    assert o.order_type == OrderType.MARKET


def test_policy_zero_position_returns_empty():
    policy = DummyPolicy(symbol="BTCUSDT")

    orders = policy.generate(
        target_position=0.0,
        portfolio_state={},
        market_data=None,
    )

    assert orders == []