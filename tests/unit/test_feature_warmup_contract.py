from __future__ import annotations

from typing import Any

from quant_engine.contracts.feature import FeatureChannelBase
from quant_engine.features.extractor import FeatureExtractor
from quant_engine.features.registry import FEATURE_REGISTRY, register_feature


@register_feature("SPY")
class SpyFeature(FeatureChannelBase):
    def __init__(self, *, name: str, symbol: str | None = None, **kwargs: Any) -> None:
        super().__init__(name=name, symbol=symbol)
        self.init_calls = 0
        self.update_calls = 0
        self.timestamps: list[int] = []
        self._value: int | None = None

    def required_window(self) -> dict[str, int]:
        return {}

    def initialize(self, context: dict[str, Any], warmup_window: int | None = None) -> None:
        self.init_calls += 1
        self._warmup_by_update(context, warmup_window, data_type="ohlcv")

    def update(self, context: dict[str, Any]) -> None:
        self.update_calls += 1
        self.timestamps.append(int(context.get("timestamp", 0)))
        self._value = self.update_calls

    def output(self) -> int | None:
        return self._value


def test_feature_extractor_warmup_does_not_call_update() -> None:
    try:
        extractor = FeatureExtractor(
            ohlcv_handlers={},
            orderbook_handlers={},
            option_chain_handlers={},
            iv_surface_handlers={},
            sentiment_handlers={},
            trades_handlers={},
            option_trades_handlers={},
            feature_config=[
                {
                    "name": "SPY_MODEL_BTCUSDT",
                    "type": "SPY",
                    "symbol": "BTCUSDT",
                }
            ],
        )
        extractor.set_interval("1s")
        extractor.set_warmup_steps(3)
        extractor.warmup(anchor_ts=4000)
        spy = extractor.channels[0]
        assert isinstance(spy, SpyFeature)
        assert spy.init_calls == 1
        assert spy.update_calls == 3
        assert spy.timestamps == [2000, 3000, 4000]

        extractor.update(timestamp=5000)
        assert spy.update_calls == 4
    finally:
        FEATURE_REGISTRY.pop("SPY", None)
