# features/ta.py
from quant_engine.contracts.feature import FeatureChannel
from ..registry import register_feature
import pandas as pd
from quant_engine.utils.logger import get_logger, log_debug

@register_feature("RSI")
class RSIFeature(FeatureChannel):
    _logger = get_logger(__name__)
    def __init__(self, symbol=None, **kwargs):
        self._symbol = symbol
        self.period = kwargs.get("period", 14)
        self._rsi = None
        self._avg_up = None
        self._avg_down = None

    @property
    def symbol(self):
        return self._symbol

    def initialize(self, context):
        data = context["ohlcv"]
        delta = data["close"].diff()
        up = delta.clip(lower=0)
        down = (-delta.clip(upper=0))

        # Wilder's smoothing initialization
        self._avg_up = up.rolling(self.period).mean().iloc[-1]
        self._avg_down = down.rolling(self.period).mean().iloc[-1]

        rs = self._avg_up / (self._avg_down + 1e-12)
        self._rsi = float(rs)

    def update(self, context):
        bar = context["ohlcv"]
        close = bar["close"].iloc[0]
        prev_close = context["realtime"].prev_close(self._symbol) if hasattr(context["realtime"], "prev_close") else None

        if prev_close is None:
            return

        delta = close - prev_close
        up = max(delta, 0)
        down = max(-delta, 0)

        # Wilder incremental smoothing
        self._avg_up = (self._avg_up * (self.period - 1) + up) / self.period
        self._avg_down = (self._avg_down * (self.period - 1) + down) / self.period

        rs = self._avg_up / (self._avg_down + 1e-12)
        self._rsi = float(rs)

    def output(self):
        assert self._rsi is not None, "RSIFeature.output() called before initialize()"
        return {"rsi": self._rsi}


@register_feature("MACD")
class MACDFeature(FeatureChannel):
    _logger = get_logger(__name__)
    def __init__(self, symbol=None, **kwargs):

        self._symbol = symbol
        self.fast = kwargs.get("fast", 12)
        self.slow = kwargs.get("slow", 26)
        self.signal = kwargs.get("signal", 9)
        self._ema_fast = None
        self._ema_slow = None
        self._macd = None

    @property
    def symbol(self):
        return self._symbol

    def initialize(self, context):
        data = context["ohlcv"]
        close = data["close"]

        self._ema_fast = close.ewm(span=self.fast, adjust=False).mean().iloc[-1]
        self._ema_slow = close.ewm(span=self.slow, adjust=False).mean().iloc[-1]
        self._macd = float(self._ema_fast - self._ema_slow)

    def update(self, context):
        bar = context["ohlcv"]
        close = bar["close"].iloc[0]

        k_fast = 2 / (self.fast + 1)
        k_slow = 2 / (self.slow + 1)

        self._ema_fast = (close - self._ema_fast) * k_fast + self._ema_fast
        self._ema_slow = (close - self._ema_slow) * k_slow + self._ema_slow
        self._macd = float(self._ema_fast - self._ema_slow)

    def output(self):
        assert self._macd is not None, "MACDFeature.output() called before initialize()"
        return {"macd": self._macd}