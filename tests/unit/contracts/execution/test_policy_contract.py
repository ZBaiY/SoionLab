from quant_engine.contracts.execution.policy import ExecutionPolicy
from quant_engine.contracts.execution.order import Order, OrderSide, OrderType


class DummyPolicy(ExecutionPolicy):
    def generate(self, target_position, portfolio_state, market_data):
        # Minimal valid Order object
        return [
            Order(
                symbol="BTCUSDT",
                side=OrderSide.BUY,
                qty=1.0,
                order_type=OrderType.MARKET,
                price=None,
                timestamp=None,
                tag="",
                extra={}
            )
        ]


def test_execution_policy_contract_returns_list_of_orders():
    p = DummyPolicy()
    orders = p.generate(
        target_position=1.0,
        portfolio_state={"positions": {}, "cash": 1000},
        market_data={"bid": 50000, "ask": 50010}
    )

    assert isinstance(orders, list)
    assert len(orders) > 0
    assert isinstance(orders[0], Order)


def test_execution_policy_contract_order_fields():
    p = DummyPolicy()
    orders = p.generate(1.0, {}, {})

    order = orders[0]
    assert hasattr(order, "symbol")
    assert hasattr(order, "qty")
    assert hasattr(order, "side")
    assert hasattr(order, "order_type")

    assert isinstance(order.symbol, str)
    assert isinstance(order.qty, (int, float))
    assert isinstance(order.side, OrderSide)
    assert isinstance(order.order_type, OrderType)
