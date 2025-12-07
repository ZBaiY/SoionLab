# execution/matching/live.py
from quant_engine.contracts.execution.matching import MatchingEngine
from .registry import register_matching
from quant_engine.utils.logger import get_logger, log_debug, log_info


@register_matching("LIVE_BINANCE")
class LiveBinanceMatchingEngine(MatchingEngine):
    def __init__(self, client):
        self.client = client
        self._logger = get_logger(__name__)

    def match(self, orders, market_data):
        log_debug(self._logger, "LiveBinanceMatchingEngine received orders", count=len(orders))

        fills = []
        for o in orders:
            log_debug(
                self._logger,
                "LiveBinanceMatchingEngine sending live order (placeholder)",
                side=o.side.value,
                qty=o.qty,
                order_type=o.order_type.value,
                price=o.price
            )

            # Placeholder: no actual exchange call
            fill = {
                "fill_price": None,
                "filled_qty": 0.0,
                "fee": 0.0,
                "slippage": o.extra.get("slippage", 0.0),
                "side": o.side.value,
                "order_type": o.order_type.value,
                "timestamp": o.timestamp
            }
            fills.append(fill)

        log_info(self._logger, "LiveBinanceMatchingEngine produced fills (placeholder)", fills=len(fills))
        return fills