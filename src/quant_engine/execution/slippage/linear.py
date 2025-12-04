# execution/slippage/linear.py
from quant_engine.contracts.execution.slippage import SlippageModel
from .registry import register_slippage


@register_slippage("LINEAR")
class LinearSlippage(SlippageModel):
    def __init__(self, impact=0.0005):
        self.impact = impact

    def apply(self, order, market_data):
        mid = (market_data["bid"] + market_data["ask"]) / 2
        slip = self.impact * order.qty
        return mid + slip if order.side == "BUY" else mid - slip