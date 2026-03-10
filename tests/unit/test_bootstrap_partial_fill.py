from __future__ import annotations

import logging
from pathlib import Path

import pytest

from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.data.ohlcv.snapshot import OHLCVSnapshot
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
    required_windows = {"ohlcv": 5}
    warmup_steps = 1

    def update(self, timestamp: int | None = None) -> dict:
        return {}

    def warmup(self, *, anchor_ts: int) -> None:
        return None

    def set_interval(self, interval: str | None) -> None:
        return None

    def set_interval_ms(self, interval_ms: int | None) -> None:
        return None


def test_preload_logs_partial_fill(caplog: pytest.LogCaptureFixture) -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    handler = OHLCVDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        mode=EngineMode.MOCK,
        data_root=data_root,
    )
    spec = EngineSpec.from_interval(mode=EngineMode.MOCK, interval="1m", symbol="BTCUSDT")
    execution_engine = ExecutionEngine(
        DummyPolicy(symbol="BTCUSDT"),
        DummyRouter(symbol="BTCUSDT"),
        DummySlippage(symbol="BTCUSDT"),
        DummyMatcher(symbol="BTCUSDT"),
    )
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
    with caplog.at_level(logging.INFO):
        engine.preload_data(anchor_ts=1_900_000_000_000)
    records = [rec for rec in caplog.records if "engine.preload.partial_fill" in rec.getMessage()]
    assert records
    assert records[0].levelno == logging.INFO


def test_preload_logs_partial_fill_warning_when_cache_already_seeded(
    caplog: pytest.LogCaptureFixture,
) -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    handler = OHLCVDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        mode=EngineMode.MOCK,
        data_root=data_root,
    )
    handler.cache.push(
        OHLCVSnapshot.from_bar_aligned(
            timestamp=60_000,
            bar={
                "data_ts": 59_999,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1.0,
            },
            symbol="BTCUSDT",
        )
    )
    spec = EngineSpec.from_interval(mode=EngineMode.MOCK, interval="1m", symbol="BTCUSDT")
    execution_engine = ExecutionEngine(
        DummyPolicy(symbol="BTCUSDT"),
        DummyRouter(symbol="BTCUSDT"),
        DummySlippage(symbol="BTCUSDT"),
        DummyMatcher(symbol="BTCUSDT"),
    )
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
    with caplog.at_level(logging.WARNING):
        engine.preload_data(anchor_ts=1_900_000_000_000)
    records = [rec for rec in caplog.records if "engine.preload.partial_fill" in rec.getMessage()]
    assert records
    assert records[0].levelno == logging.WARNING


def test_bootstrap_uses_closed_bar_visible_end_for_range(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = OHLCVDataHandler(
        symbol="BTCUSDT",
        interval="15m",
        mode=EngineMode.MOCK,
    )
    captured: dict[str, int] = {}

    def _fake_load_from_files(*, start_ts: int, end_ts: int) -> int:
        captured["start_ts"] = int(start_ts)
        captured["end_ts"] = int(end_ts)
        return 0

    monkeypatch.setattr(handler, "_load_from_files", _fake_load_from_files)
    anchor_ts = 1_766_966_400_000  # 2025-12-29T00:00:00Z
    lookback_bars = 321
    handler.bootstrap(anchor_ts=anchor_ts, lookback=lookback_bars)

    interval_ms = int(handler.interval_ms)
    expected_end = (anchor_ts // interval_ms) * interval_ms - 1
    expected_start = expected_end - (lookback_bars - 1) * interval_ms
    assert captured["end_ts"] == expected_end
    assert captured["start_ts"] == expected_start
