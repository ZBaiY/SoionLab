from quant_engine.contracts.execution.slippage import SlippageBase
from quant_engine.contracts.execution.order import Order, OrderSide, OrderType
from .registry import register_slippage
from quant_engine.utils.logger import get_logger, log_debug


@register_slippage("DEPTH")
class DepthSlippage(SlippageBase):
    def __init__(self, symbol: str, depth_key="depth"):
        self.symbol = symbol
        self.depth_key = depth_key
        self._logger = get_logger(__name__)

    def apply(self, orders, market_data):
        log_debug(self._logger, "DepthSlippage received orders", count=len(orders))

        adjusted_orders = []
        ohlcv = market_data.get("ohlcv", None) if market_data else None
        orderbook = market_data.get("orderbook", None) if market_data else None

        bid = orderbook.get_attr("best_bid") if orderbook else None
        ask = orderbook.get_attr("best_ask") if orderbook else None
        if bid is not None and ask is not None:
            mid = 0.5 * (bid + ask)
            price_source = "orderbook.mid"
            data_ts = getattr(orderbook, "data_ts", None)
        else:
            mid = ohlcv.get_attr("close") if ohlcv else None
            price_source = "ohlcv.close"
            data_ts = getattr(ohlcv, "data_ts", None)
        if mid is None:
            raise ValueError("DepthSlippage requires mid market data")
        depth = orderbook.get_attr(self.depth_key) if orderbook else None
        if depth is None or mid is None:
            raise ValueError("DepthSlippage requires depth and mid market data")

        for o in orders:
            slip_price = mid + (o.qty / (depth + 1e-8))
            log_debug(
                self._logger,
                "DepthSlippage computed adjusted price",
                side=o.side.value,
                qty=o.qty,
                depth=depth,
                mid=mid,
                adjusted_price=slip_price
            )
            # clone order with adjusted price
            new_o = Order(
                symbol=self.symbol,
                side=o.side,
                qty=o.qty,
                order_type=o.order_type,
                price=slip_price,
                timestamp=o.timestamp,
                tag=o.tag,
                extra=dict(o.extra)  # copy existing
            )
            new_o.extra["slippage"] = slip_price - (o.price if o.price is not None else mid)
            new_o.extra["pre_slip_price"] = mid
            new_o.extra["price_source"] = price_source
            new_o.extra["price_data_ts"] = data_ts
            adjusted_orders.append(new_o)

        return adjusted_orders
