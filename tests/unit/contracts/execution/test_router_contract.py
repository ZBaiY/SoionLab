

from quant_engine.contracts.execution.router import Router
from quant_engine.contracts.execution.order import Order, OrderSide, OrderType


class DummyRouter(Router):
    def route(self, orders, market_data):
        # Pass-through / identity router for contract testing
        return orders


def _make_dummy_order():
    return Order(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        qty=1.0,
        order_type=OrderType.MARKET,
        price=None,
        timestamp=None,
        tag="",
        extra={}
    )


def test_router_contract_returns_list_of_orders():
    router = DummyRouter()

    orders = [_make_dummy_order()]
    result = router.route(orders, market_data={"bid": 50000, "ask": 50010})

    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], Order)


def test_router_contract_allows_modification():
    """
    Router must be *allowed* to modify orders, but not required to.
    This test only checks that the output structure remains valid.
    """
    router = DummyRouter()
    orders = [_make_dummy_order()]

    out = router.route(orders, {})
    assert isinstance(out, list)
    assert isinstance(out[0], Order)


def test_router_contract_accepts_multiple_orders():
    router = DummyRouter()

    orders = [
        _make_dummy_order(),
        Order(
            symbol="BTCUSDT",
            side=OrderSide.SELL,
            qty=0.5,
            order_type=OrderType.LIMIT,
            price=49900.0,
            timestamp=None,
            tag="",
            extra={}
        )
    ]

    out = router.route(orders, market_data={})
    assert isinstance(out, list)
    assert len(out) == 2
    assert all(isinstance(o, Order) for o in out)