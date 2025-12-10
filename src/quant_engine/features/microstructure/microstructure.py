# src/quant_engine/features/microstructure/microstructure.py
# Microstructure feature channels (L2/L3 orderbook derived)
from quant_engine.contracts.feature import FeatureChannel
from quant_engine.contracts.feature import FeatureChannelBase
from ..registry import register_feature
from quant_engine.utils.logger import get_logger, log_debug


@register_feature("SPREAD")
class SpreadFeature(FeatureChannelBase):
    """Best bid/ask spread."""
    _logger = get_logger(__name__)
    def __init__(self, symbol=None, **kwargs):
        assert symbol is not None, "SpreadFeature requires a symbol"
        self.symbol = symbol
        self._value = None

    def initialize(self, context, warmup_window=None):
        snap = self.snapshot(context, "orderbook", symbol=self.symbol)
        if snap is None:
            self._value = None
            return
        self._value = float(snap.best_ask - snap.best_bid)

    def update(self, context):
        snap = self.snapshot(context, "orderbook", symbol=self.symbol)
        if snap is None:
            return
        self._value = float(snap.best_ask - snap.best_bid)

    def output(self):
        assert self._value is not None, "SpreadFeature.output() called before initialize()"
        return {"SPREAD": self._value}


@register_feature("IMBALANCE")
class OrderImbalanceFeature(FeatureChannelBase):
    """Orderbook imbalance = (bid_size - ask_size) / (sum)."""
    _logger = get_logger(__name__)
    def __init__(self, symbol=None, **kwargs):
        assert symbol is not None, "OrderImbalanceFeature requires a symbol"
        self.symbol = symbol
        self._value = None

    def initialize(self, context, warmup_window=None):
        snap = self.snapshot(context, "orderbook", symbol=self.symbol)
        if snap is None:
            self._value = None
            return

        num = snap.best_bid_size - snap.best_ask_size
        den = snap.best_bid_size + snap.best_ask_size + 1e-9
        self._value = float(num / den)

    def update(self, context):
        snap = self.snapshot(context, "orderbook", symbol=self.symbol)
        if snap is None:
            return

        num = snap.best_bid_size - snap.best_ask_size
        den = snap.best_bid_size + snap.best_ask_size + 1e-9
        self._value = float(num / den)

    def output(self):
        assert self._value is not None, "OrderImbalanceFeature.output() called before initialize()"
        return {"ORDER_IMBALANCE": self._value}