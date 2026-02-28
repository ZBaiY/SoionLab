from __future__ import annotations

from pathlib import Path
from collections.abc import Callable
import os

import pandas as pd
import pytest

from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.execution.engine import ExecutionEngine
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.runtime.snapshot import EngineSnapshot
from quant_engine.contracts.portfolio import PortfolioState
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
    def __init__(self, *, required_windows: dict[str, int], warmup_steps: int) -> None:
        self.required_windows = dict(required_windows)
        self.warmup_steps = int(warmup_steps)

    def update(self, timestamp: int | None = None) -> dict:
        return {}

    def warmup(self, *, anchor_ts: int) -> None:
        return None

    def set_interval(self, interval: str | None) -> None:
        return None

    def set_interval_ms(self, interval_ms: int | None) -> None:
        return None


@pytest.fixture
def deterministic_clock(monkeypatch: pytest.MonkeyPatch):
    state = {"vals": [1_700_000_000.0], "idx": 0}

    def _set(values: list[float]) -> None:
        state["vals"] = list(values)
        state["idx"] = 0

    def _time() -> float:
        vals = state["vals"]
        idx = int(state["idx"])
        if idx >= len(vals):
            return float(vals[-1])
        state["idx"] = idx + 1
        return float(vals[idx])

    monkeypatch.setattr("quant_engine.runtime.realtime.time.time", _time)
    return _set


@pytest.fixture
def noop_sleep(monkeypatch: pytest.MonkeyPatch):
    async def _sleep(_: float) -> None:
        return None

    monkeypatch.setattr("quant_engine.runtime.realtime.asyncio.sleep", _sleep)
    async def _no_lag_monitor(**kwargs):
        return None

    monkeypatch.setattr("quant_engine.runtime.realtime.loop_lag_monitor", _no_lag_monitor)


@pytest.fixture
def inline_thread_calls(monkeypatch: pytest.MonkeyPatch):
    async def _direct(fn, *args, **kwargs):
        kwargs.pop("logger", None)
        kwargs.pop("context", None)
        kwargs.pop("op", None)
        return fn(*args, **kwargs)

    monkeypatch.setattr("quant_engine.runtime.realtime.to_thread_limited", _direct)


@pytest.fixture
def make_engine() -> Callable[..., StrategyEngine]:
    data_root = Path(__file__).resolve().parents[2] / "resources"

    def _make(
        *,
        mode: EngineMode = EngineMode.MOCK,
        required_windows: dict[str, int] | None = None,
        warmup_steps: int = 1,
        include_option_chain: bool = True,
    ) -> StrategyEngine:
        spec = EngineSpec.from_interval(mode=mode, interval="1m", symbol="BTCUSDT")
        execution_engine = ExecutionEngine(
            DummyPolicy(symbol="BTCUSDT"),
            DummyRouter(symbol="BTCUSDT"),
            DummySlippage(symbol="BTCUSDT"),
            DummyMatcher(symbol="BTCUSDT"),
        )
        ohlcv = OHLCVDataHandler(symbol="BTCUSDT", interval="1m", mode=mode, data_root=data_root)
        option_handlers: dict[str, OptionChainDataHandler] = {}
        if include_option_chain:
            option_handlers["BTCUSDT"] = OptionChainDataHandler(
                symbol="BTCUSDT",
                interval="1m",
                mode=mode,
                data_root=data_root,
                preset="option_chain",
            )
        engine = StrategyEngine(
            spec=spec,
            ohlcv_handlers={"BTCUSDT": ohlcv},
            orderbook_handlers={},
            option_chain_handlers=option_handlers,
            iv_surface_handlers={},
            sentiment_handlers={},
            trades_handlers={},
            option_trades_handlers={},
            feature_extractor=_FeatureExtractor(
                required_windows=required_windows or {"ohlcv": 1},
                warmup_steps=warmup_steps,
            ),
            models={"main": DummyModel(symbol="BTCUSDT")},
            decision=DummyDecision(symbol="BTCUSDT"),
            risk_manager=DummyRisk(symbol="BTCUSDT"),
            execution_engine=execution_engine,
            portfolio_manager=DummyPortfolio(symbol="BTCUSDT"),
            guardrails=False,
        )
        return engine

    return _make


@pytest.fixture
def mock_engine(make_engine):
    return make_engine


@pytest.fixture
def fake_ohlcv_handler(make_engine):
    eng = make_engine(mode=EngineMode.MOCK, required_windows={"ohlcv": 1}, include_option_chain=False)
    return eng.ohlcv_handlers["BTCUSDT"]


@pytest.fixture
def fake_snapshot() -> EngineSnapshot:
    return EngineSnapshot(
        timestamp=0,
        mode=EngineMode.REALTIME,
        features={},
        model_outputs={},
        decision_score=None,
        target_position=None,
        fills=[],
        portfolio=PortfolioState({"symbol": "BTCUSDT"}),
    )


@pytest.fixture
def fresh_ohlcv_window() -> Callable[[int, int, int], pd.DataFrame]:
    def _build(anchor_ts: int, window: int, interval_ms: int) -> pd.DataFrame:
        rows = [
            {"data_ts": int(anchor_ts) - (window - i - 1) * int(interval_ms), "close": 1.0}
            for i in range(int(window))
        ]
        return pd.DataFrame(rows)

    return _build


@pytest.fixture
def tmp_data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_root = tmp_path / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    import ingestion.ohlcv.source as ohlcv_src
    import ingestion.option_chain.source as oc_src

    monkeypatch.setattr(ohlcv_src, "DATA_ROOT", data_root)
    monkeypatch.setattr(ohlcv_src, "_RAW_OHLCV_ROOT", data_root / "raw" / "ohlcv")
    monkeypatch.setattr(oc_src, "DATA_ROOT", data_root)
    return data_root


@pytest.fixture(autouse=True)
def _require_live_api(request):
    if request.node.get_closest_marker("live_api") is None:
        return
    if not os.environ.get("LIVE_API_TEST"):
        pytest.skip("LIVE_API_TEST env var not set — skipping live API tests")
