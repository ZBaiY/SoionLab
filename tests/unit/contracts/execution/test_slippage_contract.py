from quant_engine.contracts.execution.slippage import SlippageModel
from quant_engine.contracts.execution.order import Order, OrderSide, OrderType


class DummySlippage(SlippageModel):
    def apply(self, orders, market_data):
        # Contract test version: return orders unchanged
        return orders


def _dummy_order():
    return Order(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        qty=1.0,
        order_type=OrderType.MARKET,
        price=50000.0,
        timestamp=None,
        tag="",
        extra={}
    )


def test_slippage_contract_returns_list_of_orders():
    model = DummySlippage()
    orders = [_dummy_order()]

    out = model.apply(orders, market_data={"bid": 49990, "ask": 50010})

    assert isinstance(out, list)
    assert len(out) == 1
    assert isinstance(out[0], Order)


def test_slippage_contract_allows_price_modification():
    """
    SlippageModel is allowed to modify price (via slippage impact),
    but must return valid Order objects.
    """
    model = DummySlippage()
    orders = [_dummy_order()]

    out = model.apply(orders, market_data={})
    assert isinstance(out, list)
    assert isinstance(out[0].price, (float, type(None)))


def test_slippage_contract_multiple_orders():
    model = DummySlippage()
    orders = [
        _dummy_order(),
        Order(
            symbol="BTCUSDT",
            side=OrderSide.SELL,
            qty=0.8,
            order_type=OrderType.LIMIT,
            price=50020.0,
            timestamp=None,
            tag="",
            extra={}
        )
    ]

    out = model.apply(orders, {})

    assert isinstance(out, list)
    assert len(out) == 2
    assert all(isinstance(o, Order) for o in out)
