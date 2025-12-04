# execution/router/simple.py
from quant_engine.contracts.execution.router import Router
from quant_engine.contracts.execution.order import Order
from .registry import register_router


@register_router("SIMPLE")
class SimpleRouter(Router):
    def __init__(self):
        pass

    def route(self, orders, market_data):
        # no changes, pass-through router
        return orders