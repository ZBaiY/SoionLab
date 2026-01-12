from quant_engine.contracts.execution.slippage import SlippageBase
from quant_engine.contracts.execution.order import Order, OrderSide, OrderType
from .registry import register_slippage
from quant_engine.utils.logger import get_logger, log_debug


@register_slippage("LINEAR")
class LinearSlippage(SlippageBase):
    def __init__(self, symbol: str, impact=0.0005):
        self.symbol = symbol
        self.impact = impact
        self._logger = get_logger(__name__)

    def apply(self, orders, market_data):
        log_debug(self._logger, "LinearSlippage received orders", count=len(orders))

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
            mid = orderbook.get_attr("mid") if orderbook else None
            if mid is None:
                mid = ohlcv.get_attr("close") if ohlcv else None
                price_source = "ohlcv.close"
                data_ts = getattr(ohlcv, "data_ts", None)
            else:
                price_source = "orderbook.mid"
                data_ts = getattr(orderbook, "data_ts", None) if orderbook else None
        if mid is None:
            raise ValueError("LinearSlippage requires mid market data")

        log_debug(
            self._logger,
            "execution.price_trace",
            symbol=self.symbol,
            pre_slip_price=mid,
            price_source=price_source,
            price_data_ts=data_ts,
        )

        for o in orders:
            slip = self.impact * o.qty
            adjusted_price = mid + slip if o.side is OrderSide.BUY else mid - slip

            log_debug(
                self._logger,
                "LinearSlippage computed slippage",
                side=o.side.value,
                qty=o.qty,
                impact=self.impact,
                mid=mid,
                slip=slip,
                adjusted_price=adjusted_price
            )

            new_o = Order(
                symbol=self.symbol,
                side=o.side,
                qty=o.qty,
                order_type=o.order_type,
                price=adjusted_price,
                timestamp=o.timestamp,
                tag=o.tag,
                extra=dict(o.extra)
            )
            new_o.extra["slippage"] = slip
            new_o.extra["pre_slip_price"] = mid
            new_o.extra["price_source"] = price_source
            new_o.extra["price_data_ts"] = data_ts
            adjusted_orders.append(new_o)

        return adjusted_orders
