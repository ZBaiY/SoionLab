from quant_engine.contracts.execution.matching import MatchingEngine
from .registry import register_matching
from quant_engine.utils.logger import get_logger, log_debug, log_info


@register_matching("SIMULATED")
class SimulatedMatchingEngine(MatchingEngine):
    def __init__(self):
        self._logger = get_logger(__name__)

    def match(self, orders, market_data):
        log_debug(self._logger, "SimulatedMatchingEngine received orders", count=len(orders))

        fills = []
        mid = market_data["mid"]

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
                "timestamp": o.timestamp
            }
            fills.append(fill)

        return fills