import pytest
from typing import List

from quant_engine.contracts.execution.order import Order, OrderSide, OrderType
from quant_engine.contracts.execution.slippage import SlippageBase
from quant_engine.execution.slippage.registry import register_slippage


@register_slippage("DUMMY_SLIPPAGE")
class DummySlippage(SlippageBase):
    """Apply fixed price impact to LIMIT orders."""

    def apply(self, orders: List[Order], market_data: dict | None) -> List[Order]:
        out = []
        for o in orders:
            if o.price is not None:
                o = Order(
                    **{**o.__dict__, "price": o.price * 1.01}
                )
            out.append(o)
        return out


def test_slippage_applies_price_change():
    slippage = DummySlippage(symbol="BTCUSDT")

    orders = [
        Order("BTCUSDT", OrderSide.BUY, 1.0, OrderType.LIMIT, 100.0)
    ]

    adjusted = slippage.apply(orders, market_data=None)

    assert adjusted[0].price == pytest.approx(101.0)
    assert orders[0].price == 100.0  # purity