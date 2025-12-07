# src/quant_engine/features/microstructure/microstructure.py
# Microstructure feature channels (L2/L3 orderbook derived)
from quant_engine.contracts.feature import FeatureChannel
from ..registry import register_feature
from quant_engine.utils.logger import get_logger, log_debug


@register_feature("SPREAD")
class SpreadFeature(FeatureChannel):
    """Best bid/ask spread."""
    _logger = get_logger(__name__)
    def __init__(self, symbol=None, **kwargs):
        self._symbol = symbol
        self._value = None

    @property
    def symbol(self):
        return self._symbol

    def initialize(self, context):
        data = context["ohlcv"]
        spread = data["ask"] - data["bid"]
        self._value = float(spread.iloc[-1])

    def update(self, context):
        data = context["ohlcv"]
        spread = data["ask"].iloc[0] - data["bid"].iloc[0]
        self._value = float(spread)

    def output(self):
        assert self._value is not None, "SpreadFeature.output() called before initialize()"
        return {"spread": self._value}


@register_feature("IMBALANCE")
class OrderImbalanceFeature(FeatureChannel):
    """Orderbook imbalance = (bid_size - ask_size) / (sum)."""
    _logger = get_logger(__name__)
    def __init__(self, symbol=None, **kwargs):
        self._symbol = symbol
        self._value = None

    @property
    def symbol(self):
        return self._symbol

    def initialize(self, context):
        data = context["ohlcv"]
        num = data["bid_size"] - data["ask_size"]
        den = data["bid_size"] + data["ask_size"] + 1e-9
        imbalance = num / den
        self._value = float(imbalance.iloc[-1])

    def update(self, context):
        data = context["ohlcv"]
        bid_size = data["bid_size"].iloc[0]
        ask_size = data["ask_size"].iloc[0]
        imbalance = (bid_size - ask_size) / (bid_size + ask_size + 1e-9)
        self._value = float(imbalance)

    def output(self):
        assert self._value is not None, "OrderImbalanceFeature.output() called before initialize()"
        return {"order_imbalance": self._value}