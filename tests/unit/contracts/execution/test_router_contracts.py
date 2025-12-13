import pytest
from typing import List

from quant_engine.contracts.execution.order import Order, OrderSide, OrderType
from quant_engine.contracts.execution.router import RouterBase
from quant_engine.execution.router.registry import register_router


@register_router("DUMMY_ROUTER")
class DummyRouter(RouterBase):
    """Identity router (passes orders through)."""

    def route(self, orders: List[Order], market_data: dict | None) -> List[Order]:
        return list(orders)


def test_router_pass_through():
    router = DummyRouter(symbol="BTCUSDT")

    orders = [
        Order("BTCUSDT", OrderSide.BUY, 1.0, OrderType.MARKET, None)
    ]

    routed = router.route(orders, market_data=None)

    assert routed == orders
    assert routed is not orders  # defensive copy