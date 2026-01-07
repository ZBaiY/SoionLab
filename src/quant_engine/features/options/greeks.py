from typing import Any

from quant_engine.contracts.feature import FeatureChannelBase


class DeltaFeature(FeatureChannelBase):
    def required_window(self) -> dict[str, int]:
        # Delta requires the latest option chain snapshot
        return {"option_chain": 1}

    def initialize(self, context: dict[str, Any], warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="option_chain")

    def update(self, context: dict[str, Any]) -> None:
        return None


class GammaFeature(FeatureChannelBase):
    def required_window(self) -> dict[str, int]:
        return {"option_chain": 1}

    def initialize(self, context: dict[str, Any], warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="option_chain")

    def update(self, context: dict[str, Any]) -> None:
        return None


class VegaFeature(FeatureChannelBase):
    def required_window(self) -> dict[str, int]:
        # Vega requires both option chain + IV surface
        return {
            "option_chain": 1,
            "iv_surface": 1,
        }

    def initialize(self, context: dict[str, Any], warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="option_chain")

    def update(self, context: dict[str, Any]) -> None:
        return None


class ThetaFeature(FeatureChannelBase):
    def required_window(self) -> dict[str, int]:
        return {"option_chain": 1}

    def initialize(self, context: dict[str, Any], warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="option_chain")

    def update(self, context: dict[str, Any]) -> None:
        return None
