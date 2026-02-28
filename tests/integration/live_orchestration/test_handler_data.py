from __future__ import annotations

from pathlib import Path

import pandas as pd

from ingestion.contracts.tick import IngestionTick
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.execution.engine import ExecutionEngine
from quant_engine.runtime.driver import BaseDriver
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
    required_windows = {"ohlcv": 1, "option_chain": 1}
    warmup_steps = 1

    def update(self, timestamp: int | None = None) -> dict:
        return {}

    def warmup(self, *, anchor_ts: int) -> None:
        return None

    def set_interval(self, interval: str | None) -> None:
        return None

    def set_interval_ms(self, interval_ms: int | None) -> None:
        return None


def test_ohlcv_tick_before_anchor_is_dropped() -> None:
    data_root = Path(__file__).resolve().parents[2] / "resources"
    handler = OHLCVDataHandler(symbol="BTCUSDT", interval="1m", mode=EngineMode.REALTIME, data_root=data_root)
    ts = 1_700_000_000_000
    tick = IngestionTick(
        timestamp=ts,
        data_ts=ts,
        domain="ohlcv",
        symbol="BTCUSDT",
        payload={"data_ts": ts, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0},
        source_id=getattr(handler, "source_id", None),
    )
    handler.on_new_tick(tick)
    assert handler.last_timestamp() is None


def test_option_chain_excluded_from_gap_labels() -> None:
    data_root = Path(__file__).resolve().parents[2] / "resources"
    spec = EngineSpec.from_interval(mode=EngineMode.MOCK, interval="1m", symbol="BTCUSDT")
    execution_engine = ExecutionEngine(
        DummyPolicy(symbol="BTCUSDT"),
        DummyRouter(symbol="BTCUSDT"),
        DummySlippage(symbol="BTCUSDT"),
        DummyMatcher(symbol="BTCUSDT"),
    )
    ohlcv = OHLCVDataHandler(symbol="BTCUSDT", interval="1m", mode=EngineMode.MOCK, data_root=data_root)
    option_chain = OptionChainDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        mode=EngineMode.MOCK,
        data_root=data_root,
        preset="option_chain",
    )
    engine = StrategyEngine(
        spec=spec,
        ohlcv_handlers={"BTCUSDT": ohlcv},
        orderbook_handlers={},
        option_chain_handlers={"BTCUSDT": option_chain},
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

    class _Driver(BaseDriver):
        async def iter_timestamps(self):
            yield 0

        async def run(self):
            return None

    driver = _Driver(engine=engine, spec=spec)
    labels = driver._collect_gap_labels(target_ts=1_700_000_000_000)

    assert option_chain._should_backfill() is False
    assert all(not lbl.startswith("option_chain:") for lbl in labels)
