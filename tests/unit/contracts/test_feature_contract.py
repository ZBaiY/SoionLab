from quant_engine.contracts.feature import FeatureChannel
from typing import Dict, Any

class DummyFeature(FeatureChannel):

    def __init__(self, symbol: str | None = None):
        self._symbol = symbol
        self._value = 1.0

    @property
    def symbol(self) -> str | None:
        return self._symbol

    def initialize(self, context: Dict[str, Any]) -> None:
        # In a real implementation you'd read full window_df etc.
        self._value = 1.0

    def update(self, context: Dict[str, Any]) -> None:
        # No real logic; just show that update() is callable.
        self._value = 1.0

    def output(self) -> Dict[str, float]:
        return {"dummy": self._value}

    def required_window(self) -> int:
        return 1

def test_feature_contract_output_format():
    feat = DummyFeature()

    # --- symbol contract ---
    assert hasattr(feat, "symbol")
    assert (feat.symbol is None) or isinstance(feat.symbol, str)
    assert hasattr(feat, "_symbol")

    # --- initialize contract ---
    dummy_context_init = {
        "realtime_ohlcv": None,
        "orderbook_realtime": None,
        "realtime": None,
        "option_chain": None,
        "sentiment": None,
    }
    feat.initialize(dummy_context_init)

    # --- update contract ---
    dummy_context_update = {
        "ohlcv": None,
        "historical": None,
        "realtime": None,
        "option_chain": None,
        "sentiment": None,
    }
    feat.update(dummy_context_update)

    # --- output contract ---
    out = feat.output()
    assert isinstance(out, dict)
    assert all(isinstance(k, str) for k in out.keys())
    assert all(isinstance(v, (float, int)) for v in out.values())

    # --- required_window contract ---
    assert isinstance(feat.required_window(), int)