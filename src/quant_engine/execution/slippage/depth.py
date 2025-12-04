# execution/slippage/depth.py
from quant_engine.contracts.execution.slippage import SlippageModel
from .registry import register_slippage


@register_slippage("DEPTH")
class DepthSlippage(SlippageModel):
    def __init__(self, depth_key="depth"):
        self.depth_key = depth_key

    def apply(self, order, market_data):
        depth = market_data[self.depth_key]  # e.g. dict with volume levels
        # placeholder: realistic depth = complex model
        return market_data["mid"] + (order.qty / (depth + 1e-8))