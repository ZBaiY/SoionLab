from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pytest

from quant_engine.contracts.engine import StrategyEngineProto
from quant_engine.contracts.portfolio import PortfolioState
from quant_engine.data.contracts.protocol_realtime import OHLCVHandlerProto, RealTimeDataHandler
from quant_engine.runtime.backtest import BacktestDriver
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.runtime.realtime import RealtimeDriver
from quant_engine.runtime.snapshot import EngineSnapshot
from quant_engine.strategy.engine import StrategyEngine

import quant_engine.runtime.backtest as backtest_module

EPOCH_MS = 1_700_000_000_000


def _record(trace: list[str], name: str) -> None:
    if not trace or trace[-1] != name:
        trace.append(name)


class DummyHandler:
    def __init__(self, trace: list[str], symbol: str = "BTCUSDT") -> None:
        self.symbol = symbol
        self.interval: str = "1s"
        self.interval_ms: int = 1000
        self.bootstrap_cfg: dict[str, Any] = {}
        self._anchor_ts: int | None = None
        self._trace = trace

    def load_history(self, *, start_ts: int | None = None, end_ts: int | None = None) -> None:
        return None

    def bootstrap(self, *, anchor_ts: int | None = None, lookback=None) -> None:
        return None

    def warmup_to(self, ts: int) -> None:
        self.align_to(ts)

    def align_to(self, ts: int) -> None:
        self._anchor_ts = int(ts)

    def last_timestamp(self) -> int | None:
        return self._anchor_ts

    def get_snapshot(self, ts: int | None = None):
        _record(self._trace, "handlers")
        snap_ts = int(ts if ts is not None else (self._anchor_ts or 0))
        return {"data_ts": snap_ts, "timestamp": snap_ts}

    def window(self, ts: int | None = None, n: int = 1):
        return [self.get_snapshot(ts) for _ in range(n)]

    def on_new_tick(self, tick) -> None:
        return None

    def window_df(self, window: int | None = None):
        return []


class DummyFeatureExtractor:
    def __init__(self, trace: list[str]) -> None:
        self.required_windows = {"ohlcv": 1}
        self.warmup_steps = 1
        self._trace = trace

    def warmup(self, *, anchor_ts: int) -> None:
        return None

    def update(self, timestamp: int | None = None) -> dict[str, float]:
        _record(self._trace, "features")
        return {"DUMMY_MODEL_BTCUSDT": 1.0}

    def set_interval(self, interval: str | None) -> None:
        return None

    def set_interval_ms(self, interval_ms: int | None) -> None:
        return None


class DummyModel:
    def __init__(self, trace: list[str], symbol: str = "BTCUSDT") -> None:
        self._trace = trace
        self.symbol = symbol
        self.secondary: str | None = None
        self.required_features: set[str] = set()
        self.required_feature_types: set[str] = set()
        self.last_context: dict | None = None

    def predict_with_context(self, features: dict, context: dict) -> float:
        _record(self._trace, "models")
        self.last_context = dict(context)
        return 1.0

    def predict(self, features: dict) -> float:
        _record(self._trace, "models")
        return 1.0

    def set_required_features(self, feature_names: Iterable[str]) -> None:
        self.required_features = set(feature_names)

    def validate_features(self, available_features: set[str]) -> None:
        return None

    def validate_feature_types(self, available_feature_types: set[str]) -> None:
        return None


class DummyDecision:
    def __init__(self, trace: list[str]) -> None:
        self._trace = trace
        self.required_features: set[str] = set()
        self.required_feature_types: set[str] = set()
        self.last_context: dict | None = None

    def decide(self, context: dict) -> float:
        _record(self._trace, "decision")
        self.last_context = dict(context)
        return 0.5

    def set_required_features(self, feature_names: Iterable[str]) -> None:
        self.required_features = set(feature_names)

    def validate_features(self, available_features: set[str]) -> None:
        return None

    def validate_feature_types(self, available_feature_types: set[str]) -> None:
        return None


class DummyRisk:
    def __init__(self, trace: list[str]) -> None:
        self._trace = trace
        self.last_context: dict | None = None

    def adjust(self, size: float, context: dict) -> float:
        _record(self._trace, "risk")
        self.last_context = dict(context)
        return float(size)

    def set_required_features(self, feature_names: Iterable[str]) -> None:
        return None

    def validate_features(self, available_features: set[str]) -> None:
        return None

    def validate_feature_types(self, available_feature_types: set[str]) -> None:
        return None


class DummyExecution:
    def __init__(self, trace: list[str]) -> None:
        self._trace = trace
        self.last_call: tuple[int, float, dict] | None = None

    def execute(
        self,
        timestamp: int,
        target_position: float,
        portfolio_state: dict,
        market_data: dict | None,
    ) -> list:
        _record(self._trace, "execution")
        self.last_call = (int(timestamp), float(target_position), dict(portfolio_state))
        return [{"fill_price": 1.0, "filled_qty": 1.0, "fee": 0.0}]


class DummyPortfolio:
    def __init__(self, trace: list[str]) -> None:
        self._trace = trace
        self._state = {"cash": 1.0}
        self.symbol = "BTCUSDT"

    def apply_fill(self, fill: dict) -> None:
        _record(self._trace, "portfolio")

    def state(self) -> PortfolioState:
        return PortfolioState(dict(self._state))


def test_engine_step_order_and_context() -> None:
    trace: list[str] = []
    handler = DummyHandler(trace)
    feature_extractor = DummyFeatureExtractor(trace)
    model = DummyModel(trace)
    decision = DummyDecision(trace)
    risk = DummyRisk(trace)
    execution = DummyExecution(trace)
    portfolio = DummyPortfolio(trace)
    spec = EngineSpec.from_interval(
        mode=EngineMode.BACKTEST,
        interval="1s",
        symbol="BTCUSDT",
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
        feature_extractor=feature_extractor,
        models={"main": model},
        decision=decision,
        risk_manager=risk,
        execution_engine=execution,
        portfolio_manager=portfolio,
        guardrails=True,
    )
    engine.preload_data(anchor_ts=EPOCH_MS)
    engine.warmup_features(anchor_ts=EPOCH_MS)
    trace.clear()
    snapshot = engine.step(ts=EPOCH_MS)

    assert trace == ["handlers", "features", "models", "decision", "risk", "execution", "portfolio"]
    assert isinstance(snapshot, EngineSnapshot)
    assert model.last_context is not None
    assert decision.last_context is not None
    assert risk.last_context is not None
    assert model.last_context["timestamp"] == EPOCH_MS
    assert decision.last_context["timestamp"] == EPOCH_MS
    assert risk.last_context["timestamp"] == EPOCH_MS


class SpyEngine(StrategyEngineProto):
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.spec = EngineSpec.from_interval(
            mode=EngineMode.BACKTEST,
            interval="1s",
            symbol="BTCUSDT",
        )
        self.ohlcv_handlers: dict[str, OHLCVHandlerProto] = {}
        self.orderbook_handlers: dict[str, RealTimeDataHandler] = {}
        self.option_chain_handlers: dict[str, RealTimeDataHandler] = {}
        self.iv_surface_handlers: dict[str, RealTimeDataHandler] = {}
        self.sentiment_handlers: dict[str, RealTimeDataHandler] = {}
        self.trades_handlers: dict[str, RealTimeDataHandler] = {}
        self.option_trades_handlers: dict[str, RealTimeDataHandler] = {}
        self.engine_snapshot = EngineSnapshot(
            timestamp=0,
            mode=EngineMode.BACKTEST,
            features={},
            model_outputs={},
            decision_score=None,
            target_position=None,
            fills=[],
            market_data=None,
            portfolio=PortfolioState({}),
        )

    def bootstrap(self, *, anchor_ts: int | None = None) -> None:
        self.calls.append(("bootstrap", anchor_ts))

    def preload_data(self, *, anchor_ts: int | None = None) -> None:
        self.calls.append(("preload_data", anchor_ts))

    def load_history(self, *, start_ts: int | None = None, end_ts: int | None = None) -> None:
        self.calls.append(("load_history", start_ts, end_ts))

    def warmup_features(self, *, anchor_ts: int | None = None) -> None:
        self.calls.append(("warmup_features", anchor_ts))

    def align_to(self, ts: int) -> None:
        self.calls.append(("align_to", int(ts)))

    def ingest_tick(self, tick) -> None:
        self.calls.append(("ingest_tick", tick))

    def step(self, *, ts: int) -> EngineSnapshot:
        self.calls.append(("step", int(ts)))
        return self.engine_snapshot

    def get_snapshot(self) -> EngineSnapshot | None:
        return self.engine_snapshot

    def last_timestamp(self) -> int | None:
        return None

    def iter_shutdown_objects(self):
        return []


@pytest.mark.asyncio
async def test_backtest_lifecycle_order(monkeypatch) -> None:
    engine = SpyEngine()
    spec = EngineSpec.from_interval(
        mode=EngineMode.BACKTEST,
        interval="1s",
        symbol="BTCUSDT",
    )

    async def _direct(fn, *args, **kwargs):
        kwargs.pop("logger", None)
        kwargs.pop("context", None)
        kwargs.pop("op", None)
        return fn(*args, **kwargs)

    monkeypatch.setattr(backtest_module, "to_thread_limited", _direct)
    driver = BacktestDriver(
        engine=engine,
        spec=spec,
        start_ts=EPOCH_MS,
        end_ts=EPOCH_MS,
        tick_queue=None,
    )
    await driver.run()

    assert engine.calls[:4] == [
        ("load_history", EPOCH_MS, EPOCH_MS),
        ("warmup_features", EPOCH_MS),
        ("align_to", EPOCH_MS),
        ("step", EPOCH_MS),
    ]


class OneShotRealtimeDriver(RealtimeDriver):
    async def iter_timestamps(self):
        yield EPOCH_MS


@pytest.mark.asyncio
async def test_realtime_lifecycle_order() -> None:
    engine = SpyEngine()
    spec = EngineSpec.from_interval(
        mode=EngineMode.REALTIME,
        interval="1s",
        symbol="BTCUSDT",
        timestamp=EPOCH_MS,
    )
    driver = OneShotRealtimeDriver(engine=engine, spec=spec)
    await driver.run()

    assert engine.calls[:4] == [
        ("bootstrap", EPOCH_MS),
        ("warmup_features", EPOCH_MS),
        ("align_to", EPOCH_MS),
        ("step", EPOCH_MS),
    ]
