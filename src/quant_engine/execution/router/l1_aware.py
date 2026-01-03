from quant_engine.contracts.execution.router import RouterBase
from quant_engine.contracts.execution.order import Order, OrderSide, OrderType
from .registry import register_router
from quant_engine.utils.logger import get_logger, log_debug


@register_router("L1-AWARE")
class L1AwareRouter(RouterBase):
    def __init__(self, symbol: str):
        self.symbol = symbol
        self._logger = get_logger(__name__)

    def route(self, orders, market_data: dict[str, float]):
        log_debug(self._logger, "L1AwareRouter received orders", orders=[o.to_dict() for o in orders])
        bid = market_data["bid"]
        ask = market_data["ask"]
        log_debug(self._logger, "L1AwareRouter market data", bid=bid, ask=ask)

        routed = []
        for o in orders:
            if o.order_type == OrderType.LIMIT and o.price is None:
                if o.side == OrderSide.BUY:
                    o.price = bid
                else:
                    o.price = ask
            log_debug(self._logger, "L1AwareRouter routed order", side=o.side.value, order_type=o.order_type.value, price=o.price)
            routed.append(o)
        return routed