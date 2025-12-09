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
        # Use option-chain handlers (multi-symbol)
        chain_handlers = context.get("option_chain_handlers", [])
        chain = None
        for h in chain_handlers:
            if getattr(h, "symbol", None) == self.symbol:
                chain = h
                break
        if chain is None:
            self._iv30 = None
            return

        df = chain.latest_chain()
        if df is None or "iv_30d" not in df:
            self._iv30 = None
        else:
            self._iv30 = float(df["iv_30d"].iloc[-1])

    def update(self, context):
        chain_handlers = context.get("option_chain_handlers", [])
        chain = None
        for h in chain_handlers:
            if getattr(h, "symbol", None) == self.symbol:
                chain = h
                break
        if chain is None:
            return

        bar = chain.latest_chain()
        if bar is None or "iv_30d" not in bar:
            return
        self._iv30 = float(bar["iv_30d"].iloc[-1])

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
        chain_handlers = context.get("option_chain_handlers", [])
        chain = None
        for h in chain_handlers:
            if getattr(h, "symbol", None) == self.symbol:
                chain = h
                break
        if chain is None:
            self._skew = None
            return

        df = chain.latest_chain()
        if df is None or "iv_25d_call" not in df or "iv_25d_put" not in df:
            self._skew = None
        else:
            call_iv = df["iv_25d_call"].iloc[-1]
            put_iv = df["iv_25d_put"].iloc[-1]
            self._skew = float(call_iv - put_iv)

    def update(self, context):
        chain_handlers = context.get("option_chain_handlers", [])
        chain = None
        for h in chain_handlers:
            if getattr(h, "symbol", None) == self.symbol:
                chain = h
                break
        if chain is None:
            return

        bar = chain.latest_chain()
        if bar is None or "iv_25d_call" not in bar or "iv_25d_put" not in bar:
            return
        call_iv = bar["iv_25d_call"].iloc[-1]
        put_iv = bar["iv_25d_put"].iloc[-1]
        self._skew = float(call_iv - put_iv)

    def output(self):
        return {"IV_SKEW": self._skew}