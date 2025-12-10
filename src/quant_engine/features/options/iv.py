# src/quant_engine/features/options/iv.py
# NOTE: These IV features use OHLCV-derived IV columns.
# Option-chain–based IV surface features live in iv_surface.py.
from quant_engine.contracts.feature import FeatureChannelBase
from ..registry import register_feature
from quant_engine.utils.logger import get_logger, log_debug


##### It is redundant, we are moving this to data layer.
@register_feature("IV30")
class IV30Feature(FeatureChannelBase):
    def __init__(self, symbol=None, **kwargs):
        self._symbol = symbol
        self._iv30 = None

    @property
    def symbol(self):
        return self._symbol

    _logger = get_logger(__name__)
    """Implied Volatility 30d."""
    def initialize(self, context, warmup_window=None):
        snapshot = self.snapshot(context, "options", symbol=self.symbol)
        if snapshot is None or snapshot.chain is None:
            self._iv30 = None
            return

        df = snapshot.chain
        if "iv_30d" not in df:
            self._iv30 = None
        else:
            self._iv30 = float(df["iv_30d"].iloc[-1])

    def update(self, context):
        snapshot = self.snapshot(context, "options", symbol=self.symbol)
        if snapshot is None or snapshot.chain is None:
            return

        df = snapshot.chain
        if "iv_30d" not in df:
            return

        self._iv30 = float(df["iv_30d"].iloc[-1])

    def output(self):
        return {"IV30": self._iv30}


@register_feature("IV_SKEW")
class IVSkewFeature(FeatureChannelBase):
    def __init__(self, symbol=None, **kwargs):
        self._symbol = symbol
        self._skew = None

    @property
    def symbol(self):
        return self._symbol

    _logger = get_logger(__name__)
    """25d call - 25d put skew."""
    def initialize(self, context, warmup_window=None):
        snapshot = self.snapshot(context, "options", symbol=self.symbol)
        if snapshot is None or snapshot.chain is None:
            self._skew = None
            return

        df = snapshot.chain
        if "iv_25d_call" not in df or "iv_25d_put" not in df:
            self._skew = None
        else:
            call_iv = df["iv_25d_call"].iloc[-1]
            put_iv = df["iv_25d_put"].iloc[-1]
            self._skew = float(call_iv - put_iv)

    def update(self, context):
        snapshot = self.snapshot(context, "options", symbol=self.symbol)
        if snapshot is None or snapshot.chain is None:
            return

        df = snapshot.chain
        if "iv_25d_call" not in df or "iv_25d_put" not in df:
            return

        call_iv = df["iv_25d_call"].iloc[-1]
        put_iv = df["iv_25d_put"].iloc[-1]
        self._skew = float(call_iv - put_iv)

    def output(self):
        return {"IV_SKEW": self._skew}