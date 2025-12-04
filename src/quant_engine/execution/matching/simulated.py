# execution/matching/simulated.py
from quant_engine.contracts.execution.matching import MatchingEngine
from .registry import register_matching


@register_matching("SIMULATED")
class SimulatedMatchingEngine(MatchingEngine):
    def __init__(self):
        pass

    def execute(self, order, market_data):
        fill_price = market_data["mid"]
        fee = abs(order.qty) * 0.0004
        return {
            "fill_price": fill_price,
            "filled_qty": order.qty,
            "fee": fee,
            "slippage": fill_price - (market_data["mid"])
        }