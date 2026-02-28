from __future__ import annotations

import logging
from pathlib import Path

import pytest

from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
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
    required_windows = {"ohlcv": 2, "option_chain": 50}
    warmup_steps = 1

    def update(self, timestamp: int | None = None) -> dict:
        return {}

    def warmup(self, *, anchor_ts: int) -> None:
        return None

    def set_interval(self, interval: str | None) -> None:
        return None

    def set_interval_ms(self, interval_ms: int | None) -> None:
        return None


def test_warmup_mixed_domains_only_ohlcv_is_hard_required(caplog: pytest.LogCaptureFixture) -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    spec = EngineSpec.from_interval(mode=EngineMode.MOCK, interval="1m", symbol="BTCUSDT")
    execution_engine = ExecutionEngine(
        DummyPolicy(symbol="BTCUSDT"),
        DummyRouter(symbol="BTCUSDT"),
        DummySlippage(symbol="BTCUSDT"),
        DummyMatcher(symbol="BTCUSDT"),
    )
    engine = StrategyEngine(
        spec=spec,
        ohlcv_handlers={
            "BTCUSDT": OHLCVDataHandler(
                symbol="BTCUSDT",
                interval="1m",
                mode=EngineMode.MOCK,
                data_root=data_root,
            )
        },
        orderbook_handlers={},
        option_chain_handlers={
            "BTCUSDT": OptionChainDataHandler(
                symbol="BTCUSDT",
                interval="1m",
                mode=EngineMode.MOCK,
                data_root=data_root,
                preset="option_chain",
            )
        },
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
    anchor_ts = 1_704_067_320_000
    engine.preload_data(anchor_ts=anchor_ts)
    with caplog.at_level(logging.WARNING):
        engine.warmup_features(anchor_ts=anchor_ts)
    assert any("engine.warmup.soft_domain_insufficient" in rec.getMessage() for rec in caplog.records)

