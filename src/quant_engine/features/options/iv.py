# src/quant_engine/features/options/iv.py
# NOTE: These IV features use OHLCV-derived IV columns.
# Option-chain–based IV surface features live in iv_surface.py.
from quant_engine.contracts.feature import FeatureChannel
from ..registry import register_feature
from quant_engine.utils.logger import get_logger, log_debug


@register_feature("IV30")
class IV30Feature(FeatureChannel):
    def __init__(self, symbol=None, **kwargs):
        self._symbol = symbol
        self._iv30 = None

    @property
    def symbol(self):
        return self._symbol

    _logger = get_logger(__name__)
    """Implied Volatility 30d."""
    def initialize(self, context):
        df = context.get("ohlcv")
        if df is None or "iv_30d" not in df:
            self._iv30 = None
        else:
            self._iv30 = float(df["iv_30d"].iloc[-1])

    def update(self, context):
        bar = context.get("ohlcv")
        if bar is None or "iv_30d" not in bar:
            return
        # bar is a 1-row DataFrame in incremental mode
        self._iv30 = float(bar["iv_30d"].iloc[-1])

    def output(self):
        return {"iv30": self._iv30}


@register_feature("IV_SKEW")
class IVSkewFeature(FeatureChannel):
    def __init__(self, symbol=None, **kwargs):
        self._symbol = symbol
        self._skew = None

    @property
    def symbol(self):
        return self._symbol

    _logger = get_logger(__name__)
    """25d call - 25d put skew."""
    def initialize(self, context):
        df = context.get("ohlcv")
        if df is None or "iv_25d_call" not in df or "iv_25d_put" not in df:
            self._skew = None
        else:
            call_iv = df["iv_25d_call"].iloc[-1]
            put_iv = df["iv_25d_put"].iloc[-1]
            self._skew = float(call_iv - put_iv)

    def update(self, context):
        bar = context.get("ohlcv")
        if bar is None or "iv_25d_call" not in bar or "iv_25d_put" not in bar:
            return
        call_iv = bar["iv_25d_call"].iloc[0]
        put_iv = bar["iv_25d_put"].iloc[0]
        self._skew = float(call_iv - put_iv)

    def output(self):
        return {"iv_skew": self._skew}