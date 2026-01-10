# src/quant_engine/features/microstructure/microstructure.py
# Microstructure feature channels (L2/L3 orderbook derived)
from quant_engine.contracts.feature import FeatureChannel
from quant_engine.contracts.feature import FeatureChannelBase
from quant_engine.features.registry import register_feature

# v4 Feature Module:
# - Feature identity (name) is injected by Strategy and treated as immutable.
# - This module performs pure feature computation only.

@register_feature("MICROSPREAD")
class SpreadFeature(FeatureChannelBase):
    """Best bid/ask spread."""
    def __init__(self, *, name: str, symbol: str, **kwargs):
        super().__init__(name=name, symbol=symbol)
        self._value: float | None = None

    def required_window(self) -> dict[str, int]:
        # Snapshot-based orderbook feature
        return {"orderbook": 1}

    def initialize(self, context, warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="orderbook")

    def update(self, context):
        snap = self.get_snapshot(context, "orderbook", symbol=self.symbol)
        if snap is None:
            return
        best_ask = snap.get_attr("best_ask")
        best_bid = snap.get_attr("best_bid")
        if best_ask is None or best_bid is None:
            return
        self._value = float(best_ask - best_bid)

    def output(self):
        assert self._value is not None, "SpreadFeature.output() called before initialize()"
        return float(self._value)


@register_feature("IMBALANCE")
class OrderImbalanceFeature(FeatureChannelBase):
    """Orderbook imbalance = (bid_size - ask_size) / (sum)."""
    def __init__(self, *, name: str, symbol: str, **kwargs):
        super().__init__(name=name, symbol=symbol)
        self._value: float | None = None

    def required_window(self) -> dict[str, int]:
        # Snapshot-based orderbook feature
        return {"orderbook": 1}

    def initialize(self, context, warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="orderbook")

    def update(self, context):
        snap = self.get_snapshot(context, "orderbook", symbol=self.symbol)
        if snap is None:
            return
        best_bid_size = snap.get_attr("best_bid_size")
        best_ask_size = snap.get_attr("best_ask_size")
        if best_bid_size is None or best_ask_size is None:
            return
        num = best_bid_size - best_ask_size
        den = best_bid_size + best_ask_size + 1e-9
        self._value = float(num / den)

    def output(self):
        assert self._value is not None, "OrderImbalanceFeature.output() called before initialize()"
        return float(self._value)
