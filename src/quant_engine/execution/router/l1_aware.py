# execution/router/l1_aware.py
from quant_engine.contracts.execution.router import Router
from quant_engine.contracts.execution.order import Order
from .registry import register_router


@register_router("L1_AWARE")
class L1AwareRouter(Router):
    def __init__(self):
        pass

    def route(self, orders, market_data):
        bid = market_data["bid"]
        ask = market_data["ask"]

        routed = []
        for o in orders:
            if o.order_type == "LIMIT" and o.price is None:
                if o.side == "BUY":
                    o.price = bid
                else:
                    o.price = ask
            routed.append(o)
        return routed