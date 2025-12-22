# src/quant_engine/features/microstructure/microstructure.py
# Microstructure feature channels (L2/L3 orderbook derived)
from quant_engine.contracts.feature import FeatureChannel
from quant_engine.contracts.feature import FeatureChannelBase
from quant_engine.features.registry import register_feature

# v4 Feature Module:
# - Feature identity (name) is injected by Strategy and treated as immutable.
# - This module performs pure feature computation only.

@register_feature("SPREAD")
class SpreadFeature(FeatureChannelBase):
    """Best bid/ask spread."""
    def __init__(self, *, name: str, symbol: str):
        super().__init__(name=name, symbol=symbol)
        self._value: float | None = None

    def required_window(self) -> dict[str, int]:
        # Snapshot-based orderbook feature
        return {"orderbook": 1}

    def initialize(self, context, warmup_window=None):
        # window size is injected via context["required_windows"]
        _ = context["required_windows"]["orderbook"]
        snap = self.snapshot_dict(context, "orderbook", symbol=self.symbol)
        if snap is None:
            self._value = None
            return
        self._value = float(snap['best_ask'] - snap['best_bid'])

    def update(self, context):
        snap = self.snapshot_dict(context, "orderbook", symbol=self.symbol)
        if snap is None:
            return
        self._value = float(snap['best_ask'] - snap['best_bid'])

    def output(self):
        assert self._value is not None, "SpreadFeature.output() called before initialize()"
        return float(self._value)


@register_feature("IMBALANCE")
class OrderImbalanceFeature(FeatureChannelBase):
    """Orderbook imbalance = (bid_size - ask_size) / (sum)."""
    def __init__(self, *, name: str, symbol: str):
        super().__init__(name=name, symbol=symbol)
        self._value: float | None = None

    def required_window(self) -> dict[str, int]:
        # Snapshot-based orderbook feature
        return {"orderbook": 1}

    def initialize(self, context, warmup_window=None):
        _ = context["required_windows"]["orderbook"]
        snap = self.snapshot_dict(context, "orderbook", symbol=self.symbol)
        if snap is None:
            self._value = None
            return

        num = snap['best_bid_size'] - snap['best_ask_size']
        den = snap['best_bid_size'] + snap['best_ask_size'] + 1e-9
        self._value = float(num / den)

    def update(self, context):
        snap = self.snapshot_dict(context, "orderbook", symbol=self.symbol)
        if snap is None:
            return

        num = snap['best_bid_size'] - snap['best_ask_size']
        den = snap['best_bid_size'] + snap['best_ask_size'] + 1e-9
        self._value = float(num / den)

    def output(self):
        assert self._value is not None, "OrderImbalanceFeature.output() called before initialize()"
        return float(self._value)