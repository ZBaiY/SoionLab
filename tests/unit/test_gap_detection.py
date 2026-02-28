from __future__ import annotations

from typing import Any

import pytest

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


class _GapHandler:
    def __init__(self, last_ts: int | None, interval_ms: int = 60_000) -> None:
        self.interval_ms = interval_ms
        self._last_ts = last_ts

    def _should_backfill(self) -> bool:
        return True

    def last_timestamp(self) -> int | None:
        return self._last_ts


class _DummyEngine:
    def __init__(self) -> None:
        self.ohlcv_handlers: dict[str, Any] = {}
        self.orderbook_handlers: dict[str, Any] = {}
        self.option_chain_handlers: dict[str, Any] = {}
        self.iv_surface_handlers: dict[str, Any] = {}
        self.sentiment_handlers: dict[str, Any] = {}
        self.trades_handlers: dict[str, Any] = {}
        self.option_trades_handlers: dict[str, Any] = {}

    def iter_shutdown_objects(self):
        return []


class _Driver(BaseDriver):
    async def iter_timestamps(self):
        if False:
            yield 0

    async def run(self) -> None:
        return None


def test_collect_gap_labels_detects_missing_and_stale() -> None:
    engine = _DummyEngine()
    target_ts = 1_700_000_000_000
    engine.ohlcv_handlers = {
        "OK": _GapHandler(target_ts - 60_000),
        "GAP": _GapHandler(target_ts - 180_000),
        "NONE": _GapHandler(None),
    }
    driver = _Driver(
        engine=engine,  # type: ignore[arg-type]
        spec=EngineSpec.from_interval(mode=EngineMode.MOCK, interval="1m", symbol="BTCUSDT"),
    )
    labels = driver._collect_gap_labels(target_ts=target_ts)
    assert "ohlcv:GAP" in labels
    assert "ohlcv:NONE" in labels
    assert "ohlcv:OK" not in labels


class _Snap:
    def __init__(self, data_ts: int):
        self.data_ts = data_ts


class _HoleHandler:
    symbol = "BTCUSDT"
    interval_ms = 60_000
    interval = "1m"
    bootstrap_cfg: dict[str, Any] = {}

    def __init__(self, start: int):
        self._start = start

    def bootstrap(self, *, anchor_ts: int | None = None, lookback=None) -> None:
        return None

    def align_to(self, ts: int) -> None:
        return None

    def last_timestamp(self) -> int:
        return self._start + 9 * self.interval_ms

    def window(self, ts: int | None = None, n: int = 1):
        return [
            _Snap(self._start + 0 * self.interval_ms),
            _Snap(self._start + 1 * self.interval_ms),
            _Snap(self._start + 2 * self.interval_ms),
            _Snap(self._start + 6 * self.interval_ms),
            _Snap(self._start + 7 * self.interval_ms),
        ]

    def get_snapshot(self, ts: int | None = None):
        return None


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


def test_warmup_continuity_check_rejects_internal_hole() -> None:
    anchor_ts = 1_700_000_000_000
    handler = _HoleHandler(anchor_ts - 9 * 60_000)
    spec = EngineSpec.from_interval(mode=EngineMode.BACKTEST, interval="1m", symbol="BTCUSDT")
    execution_engine = ExecutionEngine(
        DummyPolicy(symbol="BTCUSDT"),
        DummyRouter(symbol="BTCUSDT"),
        DummySlippage(symbol="BTCUSDT"),
        DummyMatcher(symbol="BTCUSDT"),
    )
    engine = StrategyEngine(
        spec=spec,
        ohlcv_handlers={"BTCUSDT": handler},  # type: ignore[arg-type]
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
    engine._preload_done = True
    with pytest.raises(RuntimeError, match="missing history"):
        engine.warmup_features(anchor_ts=anchor_ts)

