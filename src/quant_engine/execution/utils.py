from __future__ import annotations

from decimal import Decimal, ROUND_FLOOR
from typing import Any


def to_decimal(value: Any) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def lots_from_qty(qty: float | Decimal, step_size: Decimal) -> int:
    qty_d = to_decimal(qty)
    if qty_d < 0:
        qty_d = -qty_d
    if qty_d <= 0:
        return 0
    return int((qty_d / step_size).to_integral_value(rounding=ROUND_FLOOR))


def qty_from_lots(lots: int, step_size: Decimal) -> Decimal:
    return step_size * Decimal(int(lots))


def fee_buffer(portfolio_state: dict) -> float:
    fee = float(portfolio_state.get("fee_buffer", 0.0))
    return fee if fee > 0.0 else 0.0


def price_ref_from_market(market_data: dict | None) -> float | None:
    if not market_data:
        return None
    orderbook = market_data.get("orderbook")
    if orderbook is not None:
        bid = orderbook.get_attr("best_bid") if hasattr(orderbook, "get_attr") else None
        ask = orderbook.get_attr("best_ask") if hasattr(orderbook, "get_attr") else None
        if bid is not None and ask is not None:
            return (float(bid) + float(ask)) / 2.0
        mid = orderbook.get_attr("mid") if hasattr(orderbook, "get_attr") else None
        if mid is not None:
            return float(mid)
    ohlcv = market_data.get("ohlcv")
    if ohlcv is not None:
        close = ohlcv.get_attr("close") if hasattr(ohlcv, "get_attr") else None
        if close is not None:
            return float(close)
    return None


def conservative_buy_price(market_data: dict | None, price_ref: float, slippage_bps: float) -> float:
    orderbook = market_data.get("orderbook") if market_data else None
    ask = orderbook.get_attr("best_ask") if orderbook is not None and hasattr(orderbook, "get_attr") else None
    if ask is not None:
        try:
            return max(float(ask), float(price_ref))
        except (TypeError, ValueError):
            pass
    buffer = max(0.0, float(slippage_bps)) / 1e4
    return float(price_ref) * (1.0 + buffer)
