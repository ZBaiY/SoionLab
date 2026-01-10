from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from ingestion.contracts.tick import IngestionTick
from quant_engine.contracts.feature import FeatureChannelBase
from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler
from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.data.orderbook.realtime import RealTimeOrderbookHandler
from quant_engine.execution.engine import ExecutionEngine
from quant_engine.features.extractor import FeatureExtractor
from quant_engine.features.registry import register_feature
from quant_engine.risk.engine import RiskEngine
from quant_engine.runtime.backtest import BacktestDriver
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.utils.guards import ensure_epoch_ms

from tests.helpers.fakes_runtime import (
    DummyDecision,
    DummyMatcher,
    DummyModel,
    DummyPolicy,
    DummyPortfolio,
    DummyRisk,
    DummyRouter,
    DummySlippage,
    FakeWorker,
)

EPOCH_MS = 1_700_000_000_000
FIXTURE_BASE_TS = 1_704_067_200_000
FIXTURE_INTERVAL_MS = 60_000


@register_feature("NOOP")
class NoopFeature(FeatureChannelBase):
    def __init__(
        self,
        *,
        name: str,
        symbol: str | None = None,
        required_windows: dict[str, int] | None = None,
    ) -> None:
        super().__init__(name=name, symbol=symbol)
        self._required_windows = dict(required_windows or {"ohlcv": 1})
        self._value: float = 0.0

    def required_window(self) -> dict[str, int]:
        return dict(self._required_windows)

    def initialize(self, context, warmup_window=None) -> None:
        self._value = 0.0
        self._warmup_by_update(context, warmup_window, data_type="ohlcv")

    def update(self, context) -> None:
        self._value += 1.0

    def output(self) -> float:
        return float(self._value)


class SpyFeatureExtractor(FeatureExtractor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls: list[tuple[str, int | None]] = []

    def warmup(self, *, anchor_ts: int) -> None:
        self.calls.append(("warmup", int(anchor_ts)))
        super().warmup(anchor_ts=anchor_ts)

    def update(self, timestamp: int | None = None):
        self.calls.append(("update", None if timestamp is None else int(timestamp)))
        return super().update(timestamp=timestamp)


class SpyOHLCVHandler(OHLCVDataHandler):
    def __init__(self, symbol: str, **kwargs):
        super().__init__(symbol, **kwargs)
        self.calls: list[tuple[str, object]] = []

    def bootstrap(self, *, anchor_ts: int | None = None, lookback=None) -> None:
        self.calls.append(("bootstrap", {"anchor_ts": anchor_ts, "lookback": lookback}))
        super().bootstrap(anchor_ts=anchor_ts, lookback=lookback)

    def align_to(self, ts: int) -> None:
        self.calls.append(("align_to", int(ts)))
        super().align_to(ts)

    def on_new_tick(self, tick: IngestionTick) -> None:
        self.calls.append(("on_new_tick", tick))
        super().on_new_tick(tick)

    def get_snapshot(self, ts: int | None = None):
        self.calls.append(("get_snapshot", ts))
        return super().get_snapshot(ts)

    def window(self, ts: int | None = None, n: int = 1):
        self.calls.append(("window", {"ts": ts, "n": n}))
        return super().window(ts, n)

    def last_timestamp(self) -> int | None:
        self.calls.append(("last_timestamp", None))
        return super().last_timestamp()


class SpyOrderbookHandler(RealTimeOrderbookHandler):
    def __init__(self, symbol: str, **kwargs):
        super().__init__(symbol, **kwargs)
        self.calls: list[tuple[str, object]] = []

    def bootstrap(self, *, anchor_ts: int | None = None, lookback=None) -> None:
        self.calls.append(("bootstrap", {"anchor_ts": anchor_ts, "lookback": lookback}))
        super().bootstrap(anchor_ts=anchor_ts, lookback=lookback)

    def align_to(self, ts: int) -> None:
        self.calls.append(("align_to", int(ts)))
        super().align_to(ts)

    def on_new_tick(self, tick: IngestionTick) -> None:
        self.calls.append(("on_new_tick", tick))
        super().on_new_tick(tick)

    def get_snapshot(self, ts: int | None = None):
        self.calls.append(("get_snapshot", ts))
        return super().get_snapshot(ts)

    def window(self, ts: int | None = None, n: int = 1):
        self.calls.append(("window", {"ts": ts, "n": n}))
        return super().window(ts, n)

    def last_timestamp(self) -> int | None:
        self.calls.append(("last_timestamp", None))
        return super().last_timestamp()


def _build_engine(
    *,
    feature_required_windows: dict[str, int],
    warmup_steps: int = 1,
    ohlcv_handler: OHLCVDataHandler | None = None,
    orderbook_handler: RealTimeOrderbookHandler | None = None,
    interval: str = "1s",
    data_root: Path | None = None,
) -> StrategyEngine:
    spec = EngineSpec.from_interval(
        mode=EngineMode.BACKTEST,
        interval=interval,
        symbol="BTCUSDT",
    )
    ohlcv_handler = ohlcv_handler or SpyOHLCVHandler(
        symbol="BTCUSDT",
        interval=interval,
        cache={"maxlen": 10},
        data_root=data_root,
    )
    orderbook_handler = orderbook_handler or SpyOrderbookHandler(
        symbol="BTCUSDT",
        interval=interval,
        cache={"max_snaps": 10},
        data_root=data_root,
    )
    feature_extractor = SpyFeatureExtractor(
        ohlcv_handlers={"BTCUSDT": ohlcv_handler},
        orderbook_handlers={"BTCUSDT": orderbook_handler},
        option_chain_handlers={},
        iv_surface_handlers={},
        sentiment_handlers={},
        trades_handlers={},
        option_trades_handlers={},
        feature_config=[
            {
                "name": "NOOP_TEST_BTCUSDT",
                "type": "NOOP",
                "symbol": "BTCUSDT",
                "params": {"required_windows": feature_required_windows},
            }
        ],
    )
    feature_extractor.set_warmup_steps(warmup_steps)
    feature_extractor.set_interval(interval)
    models = {"main": DummyModel(symbol="BTCUSDT")}
    decision = DummyDecision()
    risk_engine = RiskEngine([DummyRisk(symbol="BTCUSDT")], symbol="BTCUSDT")
    execution_engine = ExecutionEngine(
        DummyPolicy(symbol="BTCUSDT"),
        DummyRouter(symbol="BTCUSDT"),
        DummySlippage(symbol="BTCUSDT"),
        DummyMatcher(symbol="BTCUSDT"),
    )
    portfolio = DummyPortfolio(symbol="BTCUSDT")
    return StrategyEngine(
        spec=spec,
        ohlcv_handlers={"BTCUSDT": ohlcv_handler},
        orderbook_handlers={"BTCUSDT": orderbook_handler},
        option_chain_handlers={},
        iv_surface_handlers={},
        sentiment_handlers={},
        trades_handlers={},
        option_trades_handlers={},
        feature_extractor=feature_extractor,
        models=models,
        decision=decision,
        risk_manager=risk_engine,
        execution_engine=execution_engine,
        portfolio_manager=portfolio,
        guardrails=True,
    )


def test_engine_init_has_no_side_effects() -> None:
    handler = SpyOHLCVHandler(symbol="BTCUSDT", interval="1s", cache={"maxlen": 10})
    assert isinstance(handler, RealTimeDataHandler)
    engine = _build_engine(
        feature_required_windows={"ohlcv": 2},
        warmup_steps=1,
        ohlcv_handler=handler,
        orderbook_handler=SpyOrderbookHandler(symbol="BTCUSDT", interval="1s", cache={"max_snaps": 10}),
    )

    assert handler.calls == []
    assert isinstance(engine.feature_extractor, SpyFeatureExtractor)
    assert engine.feature_extractor.calls == []
    assert isinstance(engine.portfolio, DummyPortfolio)
    assert engine.portfolio.calls == []


def test_preload_data_only_calls_bootstrap_with_expanded_window() -> None:
    handler = SpyOHLCVHandler(symbol="BTCUSDT", interval="1s", cache={"maxlen": 10})
    engine = _build_engine(
        feature_required_windows={"ohlcv": 5},
        warmup_steps=3,
        ohlcv_handler=handler,
        orderbook_handler=SpyOrderbookHandler(symbol="BTCUSDT", interval="1s", cache={"max_snaps": 10}),
    )
    engine.preload_data(anchor_ts=EPOCH_MS)

    assert handler.calls == [("bootstrap", {"anchor_ts": EPOCH_MS, "lookback": 8})]
    assert isinstance(engine.feature_extractor, SpyFeatureExtractor)
    assert engine.feature_extractor.calls == []


def test_warmup_features_requires_history_in_backtest() -> None:
    handler = SpyOHLCVHandler(symbol="BTCUSDT", interval="1s", cache={"maxlen": 10})
    engine = _build_engine(
        feature_required_windows={"ohlcv": 2},
        warmup_steps=1,
        ohlcv_handler=handler,
        orderbook_handler=SpyOrderbookHandler(symbol="BTCUSDT", interval="1s", cache={"max_snaps": 10}),
    )
    engine.preload_data(anchor_ts=EPOCH_MS)
    with pytest.raises(RuntimeError, match="missing history"):
        engine.warmup_features(anchor_ts=EPOCH_MS)


def test_ensure_epoch_ms_coerces_seconds() -> None:
    assert ensure_epoch_ms(1_622_505_600) == 1_622_505_600_000
    assert ensure_epoch_ms(1_622_505_600_000) == 1_622_505_600_000


@pytest.mark.asyncio
async def test_backtest_driver_drain_ticks_uses_ms() -> None:
    engine = _build_engine(feature_required_windows={"ohlcv": 1})
    t0 = EPOCH_MS
    driver = BacktestDriver(
        engine=engine,
        spec=engine.spec,
        start_ts=t0,
        end_ts=t0 + 1000,
        tick_queue=asyncio.PriorityQueue(),
    )
    tick = IngestionTick(
        timestamp=t0,
        data_ts=t0,
        domain="ohlcv",
        symbol="BTCUSDT",
        payload={},
        source_id=getattr(engine.ohlcv_handlers.get("BTCUSDT"), "source_id", None),
    )
    assert isinstance(driver.tick_queue, asyncio.PriorityQueue)
    await driver.tick_queue.put((t0, 0, tick))

    seen = [t async for t in driver.drain_ticks(until_timestamp=t0 - 1)]
    assert seen == []
    seen = [t async for t in driver.drain_ticks(until_timestamp=t0)]
    assert seen == [tick]

    tick_seconds = IngestionTick(
        timestamp=1_622_505_601,
        data_ts=1_622_505_601,
        domain="ohlcv",
        symbol="BTCUSDT",
        payload={},
        source_id=getattr(engine.ohlcv_handlers.get("BTCUSDT"), "source_id", None),
    )
    await driver.tick_queue.put((1_622_505_601, 0, tick_seconds))
    seen = [t async for t in driver.drain_ticks(until_timestamp=1_622_505_600_500)]
    assert seen == []


@pytest.mark.asyncio
async def test_backtest_driver_runs_with_empty_domain_data() -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    ohlcv_handler = SpyOHLCVHandler(
        symbol="BTCUSDT",
        interval="1m",
        cache={"maxlen": 10},
        data_root=data_root,
    )
    orderbook_handler = SpyOrderbookHandler(
        symbol="BTCUSDT",
        interval="1m",
        cache={"max_snaps": 10},
        data_root=data_root,
    )
    engine = _build_engine(
        feature_required_windows={"ohlcv": 1},
        warmup_steps=1,
        ohlcv_handler=ohlcv_handler,
        orderbook_handler=orderbook_handler,
        interval="1m",
        data_root=data_root,
    )
    t0 = FIXTURE_BASE_TS + FIXTURE_INTERVAL_MS
    source_id = ohlcv_handler.source_id
    ticks = [
        IngestionTick(
            timestamp=t0,
            data_ts=t0,
            domain="ohlcv",
            symbol="BTCUSDT",
            payload={"data_ts": t0, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
            source_id=source_id,
        ),
        IngestionTick(
            timestamp=t0 + FIXTURE_INTERVAL_MS,
            data_ts=t0 + FIXTURE_INTERVAL_MS,
            domain="ohlcv",
            symbol="BTCUSDT",
            payload={
                "data_ts": t0 + FIXTURE_INTERVAL_MS,
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
            },
            source_id=source_id,
        ),
        IngestionTick(
            timestamp=t0 + 2 * FIXTURE_INTERVAL_MS,
            data_ts=t0 + 2 * FIXTURE_INTERVAL_MS,
            domain="ohlcv",
            symbol="BTCUSDT",
            payload={
                "data_ts": t0 + 2 * FIXTURE_INTERVAL_MS,
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
            },
            source_id=source_id,
        ),
    ]
    tick_queue: asyncio.PriorityQueue[tuple[int, int, IngestionTick]] = asyncio.PriorityQueue(maxsize=10)
    seq = 0

    async def emit_to_queue(tick: IngestionTick) -> None:
        nonlocal seq
        ts = ensure_epoch_ms(tick.timestamp)
        seq_key = seq
        seq += 1
        await tick_queue.put((ts, seq_key, tick))

    worker = FakeWorker(ticks)
    driver = BacktestDriver(
        engine=engine,
        spec=engine.spec,
        start_ts=t0,
        end_ts=t0 + 2 * FIXTURE_INTERVAL_MS,
        tick_queue=tick_queue,
    )

    ingest_calls: list[int] = []
    original_ingest = engine.ingest_tick

    def _record_ingest(tick: IngestionTick) -> None:
        ingest_calls.append(int(tick.timestamp))
        original_ingest(tick)

    engine.ingest_tick = _record_ingest  # type: ignore[assignment]

    await asyncio.gather(driver.run(), worker.run(emit_to_queue))

    assert ingest_calls == [
        t0,
        t0 + FIXTURE_INTERVAL_MS,
        t0 + 2 * FIXTURE_INTERVAL_MS,
    ]
    align_calls = [v for name, v in ohlcv_handler.calls if name == "align_to"]
    assert align_calls == [
        t0,
        t0 + FIXTURE_INTERVAL_MS,
        t0 + 2 * FIXTURE_INTERVAL_MS,
    ]

    snapshots = driver.snapshots
    assert [s.timestamp for s in snapshots] == [
        t0,
        t0 + FIXTURE_INTERVAL_MS,
        t0 + 2 * FIXTURE_INTERVAL_MS,
    ]
    

@pytest.mark.asyncio
async def test_tick_queue_respects_seq_for_concurrent_emitters() -> None:
    tick_queue: asyncio.PriorityQueue[tuple[int, int, IngestionTick]] = asyncio.PriorityQueue()
    seq = 0

    async def emit(tick: IngestionTick) -> None:
        nonlocal seq
        seq_key = seq
        seq += 1
        await tick_queue.put((int(tick.data_ts), seq_key, tick))

    async def worker(name: str, ticks: list[IngestionTick]) -> None:
        for tick in ticks:
            await emit(tick)
            await asyncio.sleep(0)

    base = FIXTURE_BASE_TS
    ticks_a = [
        IngestionTick(timestamp=base, data_ts=base, domain="ohlcv", symbol="A", payload={"idx": 0}),
        IngestionTick(timestamp=base + 1, data_ts=base + 1, domain="ohlcv", symbol="A", payload={"idx": 1}),
    ]
    ticks_b = [
        IngestionTick(timestamp=base + 2, data_ts=base, domain="ohlcv", symbol="B", payload={"idx": 2}),
        IngestionTick(timestamp=base + 3, data_ts=base + 2, domain="ohlcv", symbol="B", payload={"idx": 3}),
    ]

    await asyncio.gather(worker("a", ticks_a), worker("b", ticks_b))

    drained: list[tuple[int, int, IngestionTick]] = []
    while True:
        try:
            drained.append(tick_queue.get_nowait())
        except asyncio.QueueEmpty:
            break

    assert len(drained) == len(ticks_a) + len(ticks_b)
    seq_order = [seq_key for _, seq_key, _ in drained]
    assert seq_order == list(range(len(seq_order)))
    order_pairs = [(ts, seq_key) for ts, seq_key, _ in drained]
    assert order_pairs == sorted(order_pairs)


def test_run_id_format_uses_utc() -> None:
    run_backtest = Path("apps/run_backtest.py").read_text(encoding="utf-8")
    run_realtime = Path("apps/run_realtime.py").read_text(encoding="utf-8")
    assert 'strftime("%Y%m%dT%H%M%SZ")' in run_backtest
    assert 'strftime("%Y%m%dT%H%M%SZ")' in run_realtime
