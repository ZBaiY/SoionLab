# features/ta.py
from quant_engine.contracts.feature import FeatureChannelBase
from quant_engine.features.registry import register_feature
import pandas as pd
import math
from collections import deque

# v4 Feature Module:
# - Feature identity (name) is injected by Strategy and treated as immutable.
# - This module performs pure feature computation only.

@register_feature("RSI")
class RSIFeature(FeatureChannelBase):
    def __init__(self, *, name: str, symbol: str, **kwargs):
        super().__init__(name=name, symbol=symbol)
        self.window = kwargs.get("window", 14)
        if isinstance(self.window, str):
            self.window = int(self.window)
        self._rsi = None
        self._avg_up = None
        self._avg_down = None
        self._prev_close: float | None = None

    def required_window(self) -> dict[str, int]:
        return {"ohlcv": self.window + 1}

    def initialize(self, context, warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="ohlcv")

    def _seed_from_window(self, context) -> bool:
        n = context.get("required_windows", {}).get("ohlcv", self.window + 1)
        data = self.window_any_df(context, "ohlcv", n)
        if data is None or data.empty or len(data) < self.window + 1:
            return False
        delta = data["close"].diff()
        up = delta.clip(lower=0)
        down = (-delta.clip(upper=0))

        # Wilder's smoothing initialization
        self._avg_up = float(up.rolling(self.window).mean().iloc[-1])
        self._avg_down = float(down.rolling(self.window).mean().iloc[-1])

        rs = self._avg_up / (self._avg_down + 1e-12)
        self._rsi = float(100 - (100 / (1 + rs)))
        self._prev_close = float(data["close"].iloc[-1])
        return True

    def update(self, context):
        bar = self.snapshot_dict(context, "ohlcv")
        if not bar:
            return
        close = float(bar["close"])
        if self._avg_up is None or self._avg_down is None or self._prev_close is None:
            self._seed_from_window(context)
            return
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
    def __init__(self, *, name: str, symbol: str, **kwargs):
        super().__init__(name=name, symbol=symbol)
        self.fast = kwargs.get("fast", 12)
        self.slow = kwargs.get("slow", 26)
        self.signal = kwargs.get("signal", 9)
        if isinstance(self.fast, str):
            self.fast = int(self.fast)
        if isinstance(self.slow, str):
            self.slow = int(self.slow)
        if isinstance(self.signal, str):
            self.signal = int(self.signal)
        self._ema_fast = None
        self._ema_slow = None
        self._macd = None

    def required_window(self) -> dict[str, int]:
        return {"ohlcv": self.slow + 1}

    def initialize(self, context, warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="ohlcv")

    def _seed_from_window(self, context) -> bool:
        n = context.get("required_windows", {}).get("ohlcv", self.slow + 1)
        data = self.window_any_df(context, "ohlcv", n)
        if data is None or data.empty or len(data) < self.slow + 1:
            return False
        close = data["close"]

        self._ema_fast = float(close.ewm(span=self.fast, adjust=False).mean().iloc[-1])
        self._ema_slow = float(close.ewm(span=self.slow, adjust=False).mean().iloc[-1])
        self._macd = float(self._ema_fast - self._ema_slow)
        return True

    def update(self, context):
        bar = self.snapshot_dict(context, "ohlcv")
        if not bar:
            return
        close = float(bar["close"])

        k_fast = 2 / (self.fast + 1)
        k_slow = 2 / (self.slow + 1)
        if self._ema_fast is None or self._ema_slow is None:
            self._seed_from_window(context)
            return

        self._ema_fast = (close - self._ema_fast) * k_fast + self._ema_fast
        self._ema_slow = (close - self._ema_slow) * k_slow + self._ema_slow
        self._macd = float(self._ema_fast - self._ema_slow)

    def output(self):
        assert self._macd is not None, "MACDFeature.output() called before initialize()"
        return self._macd

@register_feature("ADX")
class ADXFeature(FeatureChannelBase):
    def __init__(self, *, name: str, symbol: str, **kwargs):
        super().__init__(name=name, symbol=symbol)
        self.window = kwargs.get("window", 14)
        if isinstance(self.window, str):
            self.window = int(self.window)

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
        self._warmup_by_update(context, warmup_window, data_type="ohlcv")

    def _seed_from_window(self, context) -> bool:
        n = context.get("required_windows", {}).get("ohlcv", self.window + 1)
        data = self.window_any_df(context, "ohlcv", n)
        if data is None or data.empty or len(data) < self.window + 1:
            return False
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

        dx = 100 * (abs(self._di_pos - self._di_neg) /
                    (self._di_pos + self._di_neg + 1e-12))

        self._adx = float(dx)

        self._prev_high = float(high.iloc[-1])
        self._prev_low = float(low.iloc[-1])
        self._prev_close = float(close.iloc[-1])
        return True

    def update(self, context):
        bar = self.snapshot_dict(context, "ohlcv")
        if not bar:
            return
        high = float(bar["high"])
        low = float(bar["low"])
        close = float(bar["close"])

        if self._prev_high is None or self._prev_low is None or self._prev_close is None:
            self._seed_from_window(context)
            return

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

        if self._tr is None or self._dm_pos is None or self._dm_neg is None:
            self._seed_from_window(context)
            return
        
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
        if isinstance(self.lookback, str):
            self.lookback = int(self.lookback)
        self._ret = None

    def required_window(self) -> dict[str, int]:
        return {"ohlcv": self.lookback + 1}

    def initialize(self, context, warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="ohlcv")

    def update(self, context):
        bar = self.snapshot_dict(context, "ohlcv")
        if not bar:
            return
        close = float(bar["close"])

        # need lookback window
        data = self.window_any_df(context, "ohlcv", self.lookback + 1)
        if data is None or data.empty:
            return
        prev_close = float(data["close"].iloc[0])
        self._ret = float((close / prev_close) - 1.0)

    def output(self):
        assert self._ret is not None, "ReturnFeature.output() called before initialize()"
        return self._ret
    


# --- RSI rolling mean/std features (incremental, no full series recompute) ---


def _seed_wilder_rsi_state(close: pd.Series, rsi_window: int) -> tuple[float, float, float]:
    """Seed Wilder RSI state from a close series.

    Returns:
        (avg_up, avg_down, prev_close)

    Notes:
    - Uses the first `rsi_window` deltas to initialize avg_up/avg_down
      as simple means, then caller can continue with Wilder updates.
    """
    w = int(rsi_window)
    if w <= 0:
        raise ValueError(f"rsi_window must be positive, got {w}")
    if len(close) < w + 1:
        raise ValueError(f"need at least {w + 1} closes to seed RSI, got {len(close)}")

    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = (-delta.clip(upper=0.0))

    # Seed using the first window of deltas (skip the NaN at index 0).
    avg_up = float(up.iloc[1 : w + 1].mean())
    avg_down = float(down.iloc[1 : w + 1].mean())
    prev_close = float(close.iloc[w])
    return avg_up, avg_down, prev_close


def _wilder_rsi_step(avg_up: float, avg_down: float, *, window: int, close: float, prev_close: float) -> tuple[float, float, float]:
    """One Wilder RSI update step.

    Returns:
        (new_avg_up, new_avg_down, rsi)
    """
    w = int(window)
    delta = float(close) - float(prev_close)
    up = delta if delta > 0.0 else 0.0
    down = (-delta) if delta < 0.0 else 0.0

    new_avg_up = (avg_up * (w - 1) + up) / w
    new_avg_down = (avg_down * (w - 1) + down) / w

    rs = new_avg_up / (new_avg_down + 1e-12)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return new_avg_up, new_avg_down, float(rsi)


class _RSIRollingStatBase(FeatureChannelBase):
    """Shared incremental RSI rolling-stat machinery.

    Computes RSI incrementally (Wilder) and maintains a rolling window of RSI
    values to output either mean or std.

    Params:
      - window: rolling window length for the statistic
      - rsi_window: RSI computation window (default 14)
    """

    def __init__(self, *, name: str, symbol: str, **kwargs):
        super().__init__(name=name, symbol=symbol)
        self.window = int(kwargs.get("window_rsi_rolling", kwargs.get("window_rolling", 5)))
        self.rsi_window = int(kwargs.get("rsi_window", kwargs.get("window_rsi", 14)))
        if isinstance(self.window, str):
            self.window = int(self.window)
        if isinstance(self.rsi_window, str):
            self.rsi_window = int(self.rsi_window)
        self._avg_up: float | None = None
        self._avg_down: float | None = None
        self._prev_close: float | None = None

        self._buf: deque[float] = deque()
        self._sum: float = 0.0
        self._sumsq: float = 0.0

        self._mean: float | None = None
        self._std: float | None = None

    def required_window(self) -> dict[str, int]:
        # Need enough bars to seed RSI + populate rolling stat window.
        return {"ohlcv": self.rsi_window + self.window + 2}

    def _push(self, x: float) -> None:
        self._buf.append(float(x))
        self._sum += float(x)
        self._sumsq += float(x) * float(x)
        if len(self._buf) > self.window:
            old = self._buf.popleft()
            self._sum -= float(old)
            self._sumsq -= float(old) * float(old)

        n = len(self._buf)
        if n <= 0:
            self._mean = None
            self._std = None
            return

        mean = self._sum / n
        var = (self._sumsq / n) - (mean * mean)
        if var < 0.0:
            var = 0.0
        self._mean = float(mean)
        self._std = float(math.sqrt(var))

    def initialize(self, context, warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="ohlcv")

    def _seed_from_window(self, context) -> bool:
        n = context.get("required_windows", {}).get("ohlcv", self.rsi_window + self.window + 2)
        data = self.window_any_df(context, "ohlcv", n)
        if data is None or data.empty:
            return False
        if len(data) < self.rsi_window + 1:
            return False
        close = data["close"].astype(float)

        if self.window <= 0:
            raise ValueError(f"window must be positive, got {self.window}")

        avg_up, avg_down, prev_close = _seed_wilder_rsi_state(close, self.rsi_window)
        self._avg_up = avg_up
        self._avg_down = avg_down

        for i in range(self.rsi_window + 1, len(close)):
            c = float(close.iloc[i])
            avg_up, avg_down, rsi = _wilder_rsi_step(
                avg_up,
                avg_down,
                window=self.rsi_window,
                close=c,
                prev_close=prev_close,
            )
            prev_close = c
            self._push(rsi)

        self._avg_up = avg_up
        self._avg_down = avg_down
        self._prev_close = prev_close
        return True

    def update(self, context):
        # Incremental update: compute RSI for the new close and update rolling stats.
        bar = self.snapshot_dict(context, "ohlcv")
        if not bar:
            return
        close = float(bar["close"])
        if self._avg_up is None or self._avg_down is None or self._prev_close is None:
            self._seed_from_window(context)
            return

        avg_up, avg_down, rsi = _wilder_rsi_step(
            self._avg_up,
            self._avg_down,
            window=self.rsi_window,
            close=close,
            prev_close=self._prev_close,
        )

        self._avg_up = avg_up
        self._avg_down = avg_down
        self._prev_close = close
        self._push(rsi)


@register_feature("RSI-MEAN")
class RSIRollingMeanFeature(_RSIRollingStatBase):
    def output(self):
        assert self._mean is not None, "RSI-MEAN.output() called before initialize()"
        return self._mean


@register_feature("RSI-STD")
class RSIRollingStdFeature(_RSIRollingStatBase):
    def output(self):
        assert self._std is not None, "RSI-STD.output() called before initialize()"
        return self._std
