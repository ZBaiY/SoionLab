# tests/unit/test_risk_semantics.py
import pytest

from quant_engine.risk.engine import RiskEngine
from quant_engine.risk.rule_full import FullAllocation


def test_risk_projection_long_only_clamps_negative():
    engine = RiskEngine([], symbol="BTCUSDT", risk_config={"shortable": False})
    target = engine.adjust(-1.0, {"timestamp": 1})
    assert target == 0.0


def test_risk_projection_shortable_allows_negative():
    engine = RiskEngine([], symbol="BTCUSDT", risk_config={"shortable": True})
    target = engine.adjust(-1.0, {"timestamp": 1})
    assert target == -1.0


def test_risk_rejects_score_space_rule():
    class DummyScoreRule:
        RULE_SPACE = "intent"

        def adjust(self, size: float, context: dict) -> float:
            return size

    with pytest.raises(ValueError, match="Score-space logic belongs in Decision"):
        RiskEngine([DummyScoreRule()], symbol="BTCUSDT")


def test_full_allocation_gated_by_flag():
    engine = RiskEngine([FullAllocation(symbol="BTCUSDT")], symbol="BTCUSDT")
    assert engine.rules == []

    engine_enabled = RiskEngine(
        [FullAllocation(symbol="BTCUSDT")],
        symbol="BTCUSDT",
        risk_config={"debug_full_position": True},
    )
    assert len(engine_enabled.rules) == 1
