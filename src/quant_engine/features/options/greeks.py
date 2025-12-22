from typing import Any

from quant_engine.contracts.feature import FeatureChannelBase


class DeltaFeature(FeatureChannelBase):
    def required_window(self) -> dict[str, int]:
        # Delta requires the latest option chain snapshot
        return {"option_chain": 1}

    def initialize(self, context: dict[str, Any], warmup_window=None):
        n = context["required_windows"]["option_chain"]
        chain = self.window_any(context, "option_chain", n)
        # initialization logic here


class GammaFeature(FeatureChannelBase):
    def required_window(self) -> dict[str, int]:
        return {"option_chain": 1}

    def initialize(self, context: dict[str, Any], warmup_window=None):
        n = context["required_windows"]["option_chain"]
        chain = self.window_any(context, "option_chain", n)
        # initialization logic here


class VegaFeature(FeatureChannelBase):
    def required_window(self) -> dict[str, int]:
        # Vega requires both option chain + IV surface
        return {
            "option_chain": 1,
            "iv_surface": 1,
        }

    def initialize(self, context: dict[str, Any], warmup_window=None):
        n_chain = context["required_windows"]["option_chain"]
        n_iv = context["required_windows"]["iv_surface"]
        chain = self.window_any(context, "option_chain", n_chain)
        iv = self.window_any(context, "iv_surface", n_iv)
        # initialization logic here


class ThetaFeature(FeatureChannelBase):
    def required_window(self) -> dict[str, int]:
        return {"option_chain": 1}

    def initialize(self, context: dict[str, Any], warmup_window=None):
        n = context["required_windows"]["option_chain"]
        chain = self.window_any(context, "option_chain", n)
        # initialization logic here
