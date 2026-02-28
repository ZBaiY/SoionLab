from __future__ import annotations

from pathlib import Path

from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.execution.engine import ExecutionEngine
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.strategy.engine import StrategyEngine
from tests.helpers.fakes_runtime import (
    DummyDecision,
    DummyMatcher,
    DummyModel,
    DummyPolicy,
    DummyPortfolio,
    DummyRisk,
    DummyRouter,
    DummySlippage,
)


class _FeatureExtractor:
    required_windows = {"ohlcv": 1}
    warmup_steps = 1

    def update(self, timestamp: int | None = None) -> dict:
        return {}

    def warmup(self, *, anchor_ts: int) -> None:
        return None

    def set_interval(self, interval: str | None) -> None:
        return None

    def set_interval_ms(self, interval_ms: int | None) -> None:
        return None


def test_align_to_backfill_is_deduped_per_timestamp() -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    spec = EngineSpec.from_interval(mode=EngineMode.MOCK, interval="1m", symbol="BTCUSDT")
    execution_engine = ExecutionEngine(
        DummyPolicy(symbol="BTCUSDT"),
        DummyRouter(symbol="BTCUSDT"),
        DummySlippage(symbol="BTCUSDT"),
        DummyMatcher(symbol="BTCUSDT"),
    )
    handler = OHLCVDataHandler(symbol="BTCUSDT", interval="1m", mode=EngineMode.MOCK, data_root=data_root)
    engine = StrategyEngine(
        spec=spec,
        ohlcv_handlers={"BTCUSDT": handler},
        orderbook_handlers={},
        option_chain_handlers={},
        iv_surface_handlers={},
        sentiment_handlers={},
        trades_handlers={},
        option_trades_handlers={},
        feature_extractor=_FeatureExtractor(),
        models={"main": DummyModel(symbol="BTCUSDT")},
        decision=DummyDecision(symbol="BTCUSDT"),
        risk_manager=DummyRisk(symbol="BTCUSDT"),
        execution_engine=execution_engine,
        portfolio_manager=DummyPortfolio(symbol="BTCUSDT"),
        guardrails=False,
    )
    calls = {"n": 0}

    def _count_backfill(*, target_ts: int) -> None:
        calls["n"] += 1

    handler._maybe_backfill = _count_backfill  # type: ignore[attr-defined]
    ts = 1_704_067_320_000
    engine.align_to(ts)
    engine.align_to(ts)
    engine.align_to(ts + 60_000)
    assert calls["n"] == 2

