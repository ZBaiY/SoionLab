from quant_engine.contracts.feature import FeatureChannelBase
from quant_engine.features.registry import register_feature
import pandas as pd
import numpy as np

@register_feature("ZSCORE")
class ZScoreFeature(FeatureChannelBase):
    def __init__(self, *, name: str, symbol: str, **kwargs):
        super().__init__(name=name, symbol=symbol)
        self.ref = kwargs.get("ref") or kwargs.get("secondary")
        if not self.ref:
            raise ValueError("ZSCORE feature requires ref symbol via params['ref']")

        self.lookback = int(kwargs.get("lookback", kwargs.get("window", 120)))
        self._z: float | None = None

    def required_window(self) -> dict[str, int]:
        # need rolling window of spread
        return {"ohlcv": self.lookback + 1}

    def initialize(self, context, warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="ohlcv", symbol=self.symbol)

    def update(self, context):
        # recompute using rolling window (cheap at minute freq)
        a = self.window_any_df(context, "ohlcv", self.lookback + 1, self.symbol)
        b = self.window_any_df(context, "ohlcv", self.lookback + 1, self.ref)

        if a is None or b is None:
            return

        sa = pd.Series(a["close"])
        sb = pd.Series(b["close"])
        spread = pd.Series(np.log(sa) - np.log(sb))

        mu = spread.rolling(self.lookback).mean().iloc[-1]
        sigma = spread.rolling(self.lookback).std().iloc[-1]

        if sigma is None or sigma < 1e-12:
            self._z = 0.0
        else:
            self._z = float((spread.iloc[-1] - mu) / sigma)

    def output(self):
        assert self._z is not None, "ZSCORE.output() called before initialize()"
        return self._z
    

@register_feature("SPREAD")
class SpreadFeature(FeatureChannelBase):
    def __init__(self, *, name: str, symbol: str, **kwargs):
        super().__init__(name=name, symbol=symbol)
        self.ref = kwargs.get("ref")
        if not self.ref:
            raise ValueError("SPREAD feature requires ref symbol via params['ref']")
        self._spread: float | None = None

    def required_window(self) -> dict[str, int]:
        # one bar from each symbol
        return {"ohlcv": 1}

    def initialize(self, context, warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="ohlcv", symbol=self.symbol)

    def update(self, context):
        a = self.snapshot_dict(context, "ohlcv", self.symbol)
        b = self.snapshot_dict(context, "ohlcv", self.ref)

        if not a or not b:
            return

        pa = float(a["close"])
        pb = float(b["close"])

        self._spread = float(np.log(pa) - np.log(pb))

    def output(self):
        assert self._spread is not None, "SPREAD.output() called before initialize()"
        return self._spread
