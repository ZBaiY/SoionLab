# features/ta.py
from quant_engine.contracts.feature import FeatureChannelBase
from quant_engine.features.registry import register_feature
import pandas as pd

# v4 Feature Module:
# - Feature identity (name) is injected by Strategy and treated as immutable.
# - This module performs pure feature computation only.

@register_feature("RSI")
class RSIFeature(FeatureChannelBase):
    def __init__(self, *, name: str, symbol: str, window: int = 14):
        super().__init__(name=name, symbol=symbol)
        self.window = window
        self._rsi = None
        self._avg_up = None
        self._avg_down = None
        self._prev_close: float | None = None

    def required_window(self) -> dict[str, int]:
        return {"ohlcv": self.window + 1}

    def initialize(self, context, warmup_window=None):
        n = context["required_windows"]["ohlcv"]
        data = self.window_any(context, "ohlcv", n)
        delta = data["close"].diff()
        up = delta.clip(lower=0)
        down = (-delta.clip(upper=0))

        # Wilder's smoothing initialization
        self._avg_up = float(up.rolling(self.window).mean().iloc[-1])
        self._avg_down = float(down.rolling(self.window).mean().iloc[-1])

        rs = self._avg_up / (self._avg_down + 1e-12)
        self._rsi = float(100 - (100 / (1 + rs)))
        self._prev_close = float(data["close"].iloc[-1])

    def update(self, context):
        bar = self.snapshot_dict(context, "ohlcv")
        close = float(bar["close"].iloc[0])
        if self._avg_up is None or self._avg_down is None:
            return  # initialize() not called yet
        assert self._prev_close is not None
        prev_close = self._prev_close

        delta = close - prev_close
        up = max(delta, 0)
        down = max(-delta, 0)

        # Wilder incremental smoothing
        self._avg_up = (self._avg_up * (self.window - 1) + up) / self.window
        self._avg_down = (self._avg_down * (self.window - 1) + down) / self.window

        rs = self._avg_up / (self._avg_down + 1e-12)
        self._rsi = float(100 - (100 / (1 + rs)))
        self._prev_close = close

    def output(self):
        assert self._rsi is not None, "RSIFeature.output() called before initialize()"
        return self._rsi


@register_feature("MACD")
class MACDFeature(FeatureChannelBase):
    def __init__(self, *, name: str, symbol: str, fast: int = 12, slow: int = 26, signal: int = 9):
        super().__init__(name=name, symbol=symbol)
        self.fast = fast
        self.slow = slow
        self.signal = signal
        self._ema_fast = None
        self._ema_slow = None
        self._macd = None

    def required_window(self) -> dict[str, int]:
        return {"ohlcv": self.slow + 1}

    def initialize(self, context, warmup_window=None):
        n = context["required_windows"]["ohlcv"]
        data = self.window_any(context, "ohlcv", n)
        close = data["close"]

        self._ema_fast = float(close.ewm(span=self.fast, adjust=False).mean().iloc[-1])
        self._ema_slow = float(close.ewm(span=self.slow, adjust=False).mean().iloc[-1])
        self._macd = float(self._ema_fast - self._ema_slow)

    def update(self, context):
        bar = self.snapshot_dict(context, "ohlcv")
        close = float(bar["close"].iloc[0])

        k_fast = 2 / (self.fast + 1)
        k_slow = 2 / (self.slow + 1)
        assert self._ema_fast is not None and self._ema_slow is not None, "MACDFeature.update() called before initialize()"

        self._ema_fast = (close - self._ema_fast) * k_fast + self._ema_fast
        self._ema_slow = (close - self._ema_slow) * k_slow + self._ema_slow
        self._macd = float(self._ema_fast - self._ema_slow)

    def output(self):
        assert self._macd is not None, "MACDFeature.output() called before initialize()"
        return self._macd

@register_feature("ADX")
class ADXFeature(FeatureChannelBase):
    def __init__(self, *, name: str, symbol: str, window: int = 14):
        super().__init__(name=name, symbol=symbol)
        self.window = window

        self._tr = None
        self._dm_pos = None
        self._dm_neg = None

        self._atr = None
        self._di_pos = None
        self._di_neg = None
        self._adx = None

        self._prev_high: float | None = None
        self._prev_low: float | None = None
        self._prev_close: float | None = None

    def required_window(self) -> dict[str, int]:
        return {"ohlcv": self.window + 1}

    def initialize(self, context, warmup_window=None):
        n = context["required_windows"]["ohlcv"]
        data = self.window_any(context, "ohlcv", n)
        high = data["high"]
        low = data["low"]
        close = data["close"]

        prev_close = close.shift(1)

        tr = (
            pd.concat(
                [
                    (high - low),
                    (high - prev_close).abs(),
                    (low - prev_close).abs(),
                ],
                axis=1,
            ).max(axis=1)
        )

        up_move = high.diff()
        down_move = -low.diff()

        dm_pos = ((up_move > down_move) & (up_move > 0)) * up_move
        dm_neg = ((down_move > up_move) & (down_move > 0)) * down_move

        self._tr = float(tr.rolling(self.window).sum().iloc[-1])
        self._dm_pos = float(dm_pos.rolling(self.window).sum().iloc[-1])
        self._dm_neg = float(dm_neg.rolling(self.window).sum().iloc[-1])

        self._di_pos = 100 * (self._dm_pos / (self._tr + 1e-12))
        self._di_neg = 100 * (self._dm_neg / (self._tr + 1e-12))

        assert self._di_pos is not None
        assert self._di_neg is not None

        dx = 100 * (abs(self._di_pos - self._di_neg) /
                    (self._di_pos + self._di_neg + 1e-12))

        self._adx = float(dx)

        self._prev_high = float(high.iloc[-1])
        self._prev_low = float(low.iloc[-1])
        self._prev_close = float(close.iloc[-1])

    def update(self, context):
        bar = self.snapshot_dict(context, "ohlcv")
        high = float(bar["high"].iloc[0])
        low = float(bar["low"].iloc[0])
        close = float(bar["close"].iloc[0])

        assert self._prev_high is not None
        assert self._prev_low is not None
        assert self._prev_close is not None

        prev_high = self._prev_high
        prev_low = self._prev_low
        prev_close = self._prev_close

        tr_ = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )

        up_move = high - prev_high
        down_move = prev_low - low

        dm_pos_ = up_move if (up_move > down_move and up_move > 0) else 0
        dm_neg_ = down_move if (down_move > up_move and down_move > 0) else 0

        assert self._tr is not None
        assert self._dm_pos is not None
        assert self._dm_neg is not None
        
        self._tr = self._tr - (self._tr / self.window) + tr_
        self._dm_pos = self._dm_pos - (self._dm_pos / self.window) + dm_pos_
        self._dm_neg = self._dm_neg - (self._dm_neg / self.window) + dm_neg_

        self._di_pos = 100 * (self._dm_pos / (self._tr + 1e-12))
        self._di_neg = 100 * (self._dm_neg / (self._tr + 1e-12))

        dx = 100 * (abs(self._di_pos - self._di_neg) /
                    (self._di_pos + self._di_neg + 1e-12))

        self._adx = float(dx)

        self._prev_high = high
        self._prev_low = low
        self._prev_close = close

    def output(self):
        assert self._adx is not None, "ADXFeature.output() called before initialize()"
        return self._adx

@register_feature("RETURN")
class ReturnFeature(FeatureChannelBase):
    def __init__(self, *, name: str, symbol: str, lookback: int = 1):
        super().__init__(name=name, symbol=symbol)
        self.lookback = lookback
        self._ret = None

    def required_window(self) -> dict[str, int]:
        return {"ohlcv": self.lookback + 1}

    def initialize(self, context, warmup_window=None):
        n = context["required_windows"]["ohlcv"]
        data = self.window_any(context, "ohlcv", n)
        close = data["close"]
        self._ret = float((close.iloc[-1] / close.iloc[-1 - self.lookback]) - 1.0)

    def update(self, context):
        bar = self.snapshot_dict(context, "ohlcv")
        close = float(bar["close"].iloc[0])

        # need lookback window
        data = self.window_any(context, "ohlcv", self.lookback + 1)
        prev_close = float(data["close"].iloc[0])
        self._ret = float((close / prev_close) - 1.0)

    def output(self):
        assert self._ret is not None, "ReturnFeature.output() called before initialize()"
        return self._ret