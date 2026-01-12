from quant_engine.contracts.execution.matching import MatchingBase
from .registry import register_matching
from quant_engine.utils.logger import get_logger, log_debug, log_info


@register_matching("SIMULATED")
class SimulatedMatchingEngine(MatchingBase):
    def __init__(self, symbol: str):
        self.symbol = symbol
        self._logger = get_logger(__name__)

    def match(self, orders, market_data):
        log_debug(self._logger, "SimulatedMatchingEngine received orders", count=len(orders))

        fills = []
        if market_data is None:
            raise ValueError("SimulatedMatchingEngine requires market data")
        ohlcv = market_data.get("ohlcv", None) if market_data else None
        orderbook = market_data.get("orderbook", None) if market_data else None
        

        bid = orderbook.get_attr("best_bid") if orderbook else None
        ask = orderbook.get_attr("best_ask") if orderbook else None
        if bid is not None and ask is not None:
            mid = 0.5 * (bid + ask)
        else:
            mid = ohlcv.get_attr("close") if ohlcv else None
        if mid is None:
            raise ValueError("SimulatedMatchingEngine requires mid market data")

        for o in orders:
            fill_price = o.price if o.price is not None else mid
            data_ts = None
            if orderbook is not None:
                data_ts = getattr(orderbook, "data_ts", None)
            if data_ts is None and ohlcv is not None:
                data_ts = getattr(ohlcv, "data_ts", None)

            signed_qty = float(o.qty)
            if o.side.value == "SELL":
                signed_qty = -abs(signed_qty)

            fee = abs(o.qty) * 0.0004

            log_debug(
                self._logger,
                "execution.fill_trace",
                symbol=o.symbol,
                pre_slip_price=o.extra.get("pre_slip_price"),
                price_source=o.extra.get("price_source"),
                price_data_ts=o.extra.get("price_data_ts"),
                fill_price=fill_price,
                fill_ts=o.timestamp if o.timestamp is not None else data_ts,
            )

            log_info(
                self._logger,
                "SimulatedMatchingEngine produced fill",
                fill_price=fill_price,
                filled_qty=signed_qty,
                fee=fee
            )

            fill = {
                "fill_price": fill_price,
                "filled_qty": signed_qty,
                "fee": fee,
                "slippage": o.extra.get("slippage", 0.0),
                "side": o.side.value,
                "order_type": o.order_type.value,
                "timestamp": o.timestamp if o.timestamp is not None else data_ts,
                "symbol": o.symbol,
            }
            fills.append(fill)

        return fills
