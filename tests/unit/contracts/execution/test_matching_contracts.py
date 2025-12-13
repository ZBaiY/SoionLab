import pytest
from typing import List, Dict

from quant_engine.contracts.execution.order import Order, OrderSide, OrderType
from quant_engine.contracts.execution.matching import MatchingBase
from quant_engine.execution.matching.registry import register_matching


@register_matching("DUMMY_MATCHING")
class DummyMatching(MatchingBase):
    """Instant full fill at order price or market close."""

    def match(self, orders: List[Order], market_data: dict | None) -> List[Dict]:
        fills = []
        for o in orders:
            price = o.price or market_data["close"] if market_data else 0.0
            fills.append({
                "symbol": o.symbol,
                "side": o.side.value,
                "qty": o.qty,
                "price": price,
            })
        return fills


def test_matching_produces_fills():
    matcher = DummyMatching(symbol="BTCUSDT")

    orders = [
        Order("BTCUSDT", OrderSide.BUY, 1.0, OrderType.MARKET, None)
    ]

    market_data = {"close": 100.0}

    fills = matcher.match(orders, market_data)

    assert len(fills) == 1
    assert fills[0]["price"] == 100.0