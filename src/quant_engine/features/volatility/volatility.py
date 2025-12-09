# src/quant_engine/features/volatility.py
import pandas as pd
from quant_engine.contracts.feature import FeatureChannelBase
from ..registry import register_feature
from quant_engine.utils.logger import get_logger, log_debug


def _extract_last(bar: pd.DataFrame, col: str):
    """
    Safely extract the last value of a column for either:
      - single-row DataFrame
      - multi-row DataFrame
      - Series
    """
    if isinstance(bar, pd.Series):
        return bar[col] if col in bar else bar.iloc[-1]
    if isinstance(bar, pd.DataFrame):
        if col in bar:
            return bar[col].iloc[-1]
        return bar.iloc[-1]
    raise TypeError("latest_bar() returned unsupported type")


@register_feature("ATR")
class ATRFeature(FeatureChannelBase):
    _logger = get_logger(__name__)
    """Average True Range."""
    def __init__(self, symbol=None, **kwargs):
        assert symbol is not None, "ATRFeature requires a symbol"
        self.symbol = symbol
        self.period = kwargs.get("period", 14)
        self._atr = None
        self._prev_close = None

    def required_window(self) -> int:
        return self.period + 1

    def initialize(self, context, warmup_window=None):
        df = self.window(context, max(warmup_window, self.period + 1) if warmup_window else self.period + 1)
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        self._atr = tr.rolling(self.period).mean().iloc[-1]
        # initialize prev_close for incremental updates
        self._prev_close = _extract_last(df, "close")

    def update(self, context):
        bar = self.latest_bar(context)
        assert self._prev_close is not None, "ATRFeature.update() called before initialize()"
        prev_close = self._prev_close
        high = _extract_last(bar, "high")
        low = _extract_last(bar, "low")
        close = _extract_last(bar, "close")

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
        return {"ATR": float(self._atr)}


@register_feature("REALIZED_VOL")
class RealizedVolFeature(FeatureChannelBase):
    _logger = get_logger(__name__)
    """Realized volatility via daily returns."""
    def __init__(self, symbol=None, **kwargs):
        assert symbol is not None, "RealizedVolFeature requires a symbol"
        self.symbol = symbol
        self.window_size = kwargs.get("window", 30)
        self._returns_window = []
        self._vol = None
        self._prev_close = None

    def required_window(self) -> int:
        return self.window_size + 1

    def initialize(self, context, warmup_window=None):
        window_len = max(warmup_window, self.window_size + 1) if warmup_window else self.window_size + 1
        df = self.window(context, window_len)

        returns = df["close"].pct_change().dropna()
        self._returns_window = list(returns.iloc[-self.window_size:])
        self._vol = pd.Series(self._returns_window).std()

        # initialize prev_close for incremental updates
        self._prev_close = _extract_last(df, "close")

    def update(self, context):
        bar = self.latest_bar(context)
        assert self._prev_close is not None, "RealizedVolFeature.update() called before initialize()"
        prev_close = self._prev_close
        close = _extract_last(bar, "close")

        ret = (close - prev_close) / prev_close
        self._returns_window.append(ret)
        if len(self._returns_window) > self.window_size:
            self._returns_window.pop(0)

        self._vol = pd.Series(self._returns_window).std()
        self._prev_close = close

    def output(self):
        assert self._vol is not None, "RealizedVolFeature.output() called before initialize()"
        return {"REALIZED_VOL": float(self._vol)}