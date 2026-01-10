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
        if mid is None:
            mid = ohlcv.get_attr("close") if ohlcv else None
        if mid is None:
            raise ValueError("SimulatedMatchingEngine requires mid market data")

        for o in orders:
            fill_price = mid
            fee = abs(o.qty) * 0.0004

            log_info(
                self._logger,
                "SimulatedMatchingEngine produced fill",
                fill_price=fill_price,
                filled_qty=o.qty,
                fee=fee
            )

            fill = {
                "fill_price": fill_price,
                "filled_qty": o.qty,
                "fee": fee,
                "slippage": o.extra.get("slippage", 0.0),
                "side": o.side.value,
                "order_type": o.order_type.value,
                "timestamp": o.timestamp,
                "symbol": o.symbol,
            }
            fills.append(fill)

        return fills
