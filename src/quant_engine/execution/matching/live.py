# execution/matching/live.py
from quant_engine.contracts.execution.matching import MatchingEngine
from .registry import register_matching


@register_matching("LIVE_BINANCE")
class LiveBinanceMatchingEngine(MatchingEngine):
    def __init__(self, client):
        self.client = client

    def execute(self, order, market_data):
        """
        send order to exchange
        wait for fill
        return fill info
        """
        # placeholder
        return {
            "fill_price": None,
            "filled_qty": 0,
            "fee": 0,
            "slippage": 0
        }