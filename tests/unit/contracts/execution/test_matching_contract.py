from quant_engine.contracts.execution.matching import MatchingEngine
from quant_engine.contracts.execution.order import Order, OrderSide, OrderType


class DummyMatchingEngine(MatchingEngine):
    def match(self, orders, market_data):
        # Minimal valid fill structure for contract compliance
        fills = []
        for o in orders:
            fills.append({
                "fill_price": o.price if o.price is not None else market_data.get("mid", 0.0),
                "filled_qty": o.qty,
                "fee": 0.0,
                "slippage": 0.0,
                "side": o.side,
                "order_type": o.order_type,
                "timestamp": market_data.get("timestamp", 0.0),
            })
        return fills


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


def test_matching_contract_returns_list_of_fills():
    engine = DummyMatchingEngine()
    orders = [_dummy_order()]
    mkt = {"mid": 50005.0, "timestamp": 123456.0}

    result = engine.match(orders, mkt)

    assert isinstance(result, list)
    assert len(result) == 1
    fill = result[0]

    assert isinstance(fill, dict)
    assert "fill_price" in fill
    assert "filled_qty" in fill
    assert "fee" in fill
    assert "slippage" in fill
    assert "side" in fill
    assert "order_type" in fill
    assert "timestamp" in fill

    assert isinstance(fill["fill_price"], (float, int))
    assert isinstance(fill["filled_qty"], (float, int))


def test_matching_contract_multiple_orders():
    engine = DummyMatchingEngine()

    orders = [
        _dummy_order(),
        Order(
            symbol="BTCUSDT",
            side=OrderSide.SELL,
            qty=0.5,
            order_type=OrderType.LIMIT,
            price=50010.0,
            timestamp=None,
            tag="",
            extra={}
        )
    ]

    out = engine.match(orders, {"mid": 50000, "timestamp": 111.0})

    assert isinstance(out, list)
    assert len(out) == 2

    for fill in out:
        assert isinstance(fill, dict)
        assert isinstance(fill["fill_price"], (float, int))
        assert isinstance(fill["filled_qty"], (float, int))
        assert isinstance(fill["timestamp"], (float, int))
