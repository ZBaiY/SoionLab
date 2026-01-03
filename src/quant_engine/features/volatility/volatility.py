# src/quant_engine/features/volatility.py
import pandas as pd
from quant_engine.contracts.feature import FeatureChannelBase
from quant_engine.features.registry import register_feature
# v4 Feature Module:
# - Feature identity (name) is injected by Strategy and treated as immutable.
# - This module performs pure feature computation only.


def _extract_last(bar: pd.DataFrame | pd.Series, col: str) -> float:
    """
    Safely extract the last scalar value of a column and return it as float.
    """
    if isinstance(bar, pd.Series):
        val = bar[col] if col in bar else bar.iloc[-1]
    elif isinstance(bar, pd.DataFrame):
        if col in bar:
            val = bar[col].iloc[-1]
        else:
            val = bar.iloc[-1]
    else:
        raise TypeError("latest_bar() returned unsupported type")
    assert isinstance(val, (float, int)), "Extracted value is not a float or int"
    return float(val)


@register_feature("ATR")
class ATRFeature(FeatureChannelBase):
    """Average True Range."""
    def __init__(self, *, name: str, symbol: str, **kwargs):
        super().__init__(name=name, symbol=symbol)
        self.window = kwargs.get("window", 14)
        if isinstance(self.window, str):
            self.window = int(self.window)
        self._atr: float | None = None
        self._prev_close: float | None = None

    def required_window(self) -> dict[str, int]:
        # ATR depends only on OHLCV bars
        return {"ohlcv": self.window + 1}

    def initialize(self, context, warmup_window=None):
        n = context["required_windows"]["ohlcv"]
        df = self.window_any(context, "ohlcv", n)
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        self._atr = float(tr.rolling(self.window).mean().iloc[-1])
        # initialize prev_close for incremental updates
        self._prev_close = float(_extract_last(df, "close"))

    def update(self, context):
        bar = self.snapshot_dict(context, "ohlcv")
        bar = pd.DataFrame([bar])  # ensure single-row DataFrame
        assert self._prev_close is not None, "ATRFeature.update() called before initialize()"
        prev_close = self._prev_close
        high: float = _extract_last(bar, "high")
        low: float = _extract_last(bar, "low")
        close: float = _extract_last(bar, "close")

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        # incremental ATR: ATR_t = (ATR_{t-1}*(n-1) + TR_t) / n

        assert self._atr is not None, "ATRFeature.update() called before initialize()"
        self._atr = (self._atr * (self.window - 1) + tr) / self.window
        self._prev_close = close

    def output(self) -> float:
        assert self._atr is not None, "ATRFeature.output() called before initialize()"
        return float(self._atr)


@register_feature("REALIZED_VOL")
class RealizedVolFeature(FeatureChannelBase):
    """Realized volatility via daily returns."""
    def __init__(self, *, name: str, symbol: str, **kwargs):
        super().__init__(name=name, symbol=symbol)
        self.window = kwargs.get("window", 30)
        if isinstance(self.window, str):
            self.window = int(self.window)
        self._returns_window: list[float] = []
        self._vol: float | None = None
        self._prev_close: float | None = None

    def required_window(self) -> dict[str, int]:
        # Realized volatility depends only on OHLCV bars
        return {"ohlcv": self.window + 1}

    def initialize(self, context, warmup_window=None):
        n = context["required_windows"]["ohlcv"]
        df = self.window_any(context, "ohlcv", n)

        returns = df["close"].pct_change().dropna()
        self._returns_window = list(returns.iloc[-self.window:])
        self._vol = float(pd.Series(self._returns_window).std())

        # initialize prev_close for incremental updates
        self._prev_close = float(_extract_last(df, "close"))

    def update(self, context):
        bar = self.snapshot_dict(context, "ohlcv")
        bar = pd.DataFrame([bar])  # ensure single-row DataFrame
        assert self._prev_close is not None, "RealizedVolFeature.update() called before initialize()"
        prev_close = self._prev_close
        close: float = _extract_last(bar, "close")

        ret = (close - prev_close) / prev_close
        self._returns_window.append(ret)
        if len(self._returns_window) > self.window:
            self._returns_window.pop(0)

        self._vol = float(pd.Series(self._returns_window).std())
        self._prev_close = close

    def output(self) -> float:
        assert self._vol is not None, "RealizedVolFeature.output() called before initialize()"
        return float(self._vol)