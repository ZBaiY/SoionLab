# src/quant_engine/features/volatility.py
import pandas as pd
from quant_engine.contracts.feature import FeatureChannel
from ..registry import register_feature
from quant_engine.utils.logger import get_logger, log_debug


@register_feature("ATR")
class ATRFeature(FeatureChannel):
    _logger = get_logger(__name__)
    """Average True Range."""
    def __init__(self, symbol=None, **kwargs):
        self._symbol = symbol
        self.period = kwargs.get("period", 14)
        self._atr = None
        self._prev_close = None

    @property
    def symbol(self):
        return self._symbol

    def initialize(self, context):
        df = context["ohlcv"]
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        self._atr = tr.rolling(self.period).mean().iloc[-1]
        # initialize prev_close for incremental updates
        self._prev_close = df["close"].iloc[-1]

    def update(self, context):
        bar = context["ohlcv"]    # single-row DataFrame
        assert self._prev_close is not None, "ATRFeature.update() called before initialize()"
        prev_close: float = self._prev_close
        high = bar["high"].iloc[0]
        low = bar["low"].iloc[0]
        close = bar["close"].iloc[0]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        # incremental ATR: ATR_t = (ATR_{t-1}*(n-1) + TR_t) / n
        self._atr = (self._atr * (self.period - 1) + tr) / self.period
        self._prev_close = close

    def output(self):
        assert self._atr is not None, "ATRFeature.output() called before initialize()"
        return {"atr": float(self._atr)}


@register_feature("REALIZED_VOL")
class RealizedVolFeature(FeatureChannel):
    _logger = get_logger(__name__)
    """Realized volatility via daily returns."""
    def __init__(self, symbol=None, **kwargs):
        self._symbol = symbol
        self.window = kwargs.get("window", 30)
        self._returns_window = []
        self._vol = None
        self._prev_close = None

    @property
    def symbol(self):
        return self._symbol

    def initialize(self, context):
        df = context["ohlcv"]
        returns = df["close"].pct_change().dropna()
        self._returns_window = list(returns.iloc[-self.window:])
        self._vol = pd.Series(self._returns_window).std()
        # initialize prev_close for incremental updates
        self._prev_close = df["close"].iloc[-1]

    def update(self, context):
        bar = context["ohlcv"]
        assert self._prev_close is not None, "RealizedVolFeature.update() called before initialize()"
        prev_close: float = self._prev_close
        close = bar["close"].iloc[0]

        ret = (close - prev_close) / prev_close
        self._returns_window.append(ret)
        if len(self._returns_window) > self.window:
            self._returns_window.pop(0)

        self._vol = pd.Series(self._returns_window).std()
        self._prev_close = close

    def output(self):
        assert self._vol is not None, "RealizedVolFeature.output() called before initialize()"
        return {"realized_vol": float(self._vol)}