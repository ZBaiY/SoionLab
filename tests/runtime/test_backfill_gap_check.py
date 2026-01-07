from __future__ import annotations

from pathlib import Path

import pytest
import pandas as pd

from ingestion.contracts.tick import IngestionTick
from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer
from ingestion.ohlcv.source import OHLCVRESTSource
from ingestion.ohlcv.worker import OHLCVWorker
import ingestion.ohlcv.source as ohlcv_source
from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.derivatives.iv.iv_handler import IVSurfaceDataHandler
from quant_engine.execution.engine import ExecutionEngine
from quant_engine.runtime.mock import MockDriver
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

BASE_TS = 1_704_067_200_000  # 2024-01-01T00:00:00Z
INTERVAL_MS = 60_000


class DummyFeatureExtractor:
    def __init__(
        self,
        *,
        required_windows: dict[str, int] | None = None,
        warmup_steps: int = 1,
    ) -> None:
        self.required_windows = dict(required_windows or {"ohlcv": 1})
        self.warmup_steps = int(warmup_steps)

    def update(self, timestamp: int | None = None) -> dict:
        return {}

    def warmup(self, *, anchor_ts: int) -> None:
        return None

    def set_interval(self, interval: str | None) -> None:
        return None

    def set_interval_ms(self, interval_ms: int | None) -> None:
        return None


def _build_engine(
    handler: OHLCVDataHandler,
    *,
    mode: EngineMode,
    required_windows: dict[str, int] | None = None,
) -> StrategyEngine:
    spec = EngineSpec.from_interval(mode=mode, interval="1m", symbol="BTCUSDT")
    execution_engine = ExecutionEngine(
        DummyPolicy(symbol="BTCUSDT"),
        DummyRouter(symbol="BTCUSDT"),
        DummySlippage(symbol="BTCUSDT"),
        DummyMatcher(symbol="BTCUSDT"),
    )
    return StrategyEngine(
        spec=spec,
        ohlcv_handlers={"BTCUSDT": handler},
        orderbook_handlers={},
        option_chain_handlers={},
        iv_surface_handlers={},
        sentiment_handlers={},
        trades_handlers={},
        option_trades_handlers={},
        feature_extractor=DummyFeatureExtractor(required_windows=required_windows),
        models={"main": DummyModel(symbol="BTCUSDT")},
        decision=DummyDecision(symbol="BTCUSDT"),
        risk_manager=DummyRisk(symbol="BTCUSDT"),
        execution_engine=execution_engine,
        portfolio_manager=DummyPortfolio(symbol="BTCUSDT"),
        guardrails=True,
    )


def _make_tick(ts: int) -> IngestionTick:
    return IngestionTick(
        timestamp=int(ts),
        data_ts=int(ts),
        domain="ohlcv",
        symbol="BTCUSDT",
        payload={
            "data_ts": int(ts),
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1.0,
        },
    )


def _make_ohlcv_backfill_worker(tmp_path, monkeypatch, *, symbol: str = "BTCUSDT", interval: str = "1m"):
    monkeypatch.setattr(ohlcv_source, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ohlcv_source, "_RAW_OHLCV_ROOT", tmp_path / "raw" / "ohlcv")
    backfill_calls: list[tuple[int, int]] = []

    def backfill_fn(*, start_ts: int, end_ts: int):
        backfill_calls.append((int(start_ts), int(end_ts)))
        rows: list[dict[str, float | int]] = []
        ts = int(start_ts)
        while ts <= int(end_ts):
            rows.append(
                {
                    "data_ts": int(ts),
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 1.0,
                }
            )
            ts += INTERVAL_MS
        return rows

    fetch_source = OHLCVRESTSource(fetch_fn=lambda: [], backfill_fn=backfill_fn, poll_interval_ms=1000)
    raw_root = tmp_path / "raw" / "ohlcv"
    normalizer = BinanceOHLCVNormalizer(symbol=symbol)
    worker = OHLCVWorker(
        source=fetch_source,
        fetch_source=fetch_source,
        normalizer=normalizer,
        symbol=symbol,
        interval=interval,
    )
    return worker, raw_root, backfill_calls


def test_ohlcv_bootstrap_truncates_to_cache() -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    handler = OHLCVDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        cache={"maxlen": 3},
        mode=EngineMode.MOCK,
        data_root=data_root,
    )
    anchor_ts = BASE_TS + 4 * INTERVAL_MS
    handler.bootstrap(anchor_ts=anchor_ts, lookback=5)

    snaps = list(handler.cache.window(10))
    assert [s.data_ts for s in snaps] == [
        BASE_TS + 2 * INTERVAL_MS,
        BASE_TS + 3 * INTERVAL_MS,
        BASE_TS + 4 * INTERVAL_MS,
    ]


def test_gap_backfill_in_mock_mode(tmp_path, monkeypatch) -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    worker, raw_root, backfill_calls = _make_ohlcv_backfill_worker(tmp_path, monkeypatch)
    handler = OHLCVDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        cache={"maxlen": 10},
        mode=EngineMode.MOCK,
        data_root=data_root,
    )
    handler.set_external_source(worker, emit=handler.on_new_tick)
    engine = _build_engine(handler, mode=EngineMode.MOCK)

    seed_ts = BASE_TS + 2 * INTERVAL_MS
    handler.align_to(seed_ts)
    handler.on_new_tick(_make_tick(seed_ts))

    target_ts = BASE_TS + 5 * INTERVAL_MS
    engine.align_to(target_ts)

    last_ts = handler.last_timestamp()
    assert last_ts is not None
    assert int(last_ts) >= int(target_ts) - int(handler.interval_ms)
    assert backfill_calls == [
        (seed_ts + INTERVAL_MS, target_ts),
    ]
    raw_path = raw_root / "BTCUSDT" / "1m" / "2024.parquet"
    assert raw_path.exists()
    df = pd.read_parquet(raw_path)
    expected_rows = int((target_ts - (seed_ts + INTERVAL_MS)) / INTERVAL_MS) + 1
    assert len(df) == expected_rows

    ordered = [s.data_ts for s in handler.cache.window(10)]
    assert ordered == sorted(ordered)


def test_warmup_backtest_raises_without_history(tmp_path, monkeypatch) -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    worker, _raw_root, backfill_calls = _make_ohlcv_backfill_worker(tmp_path, monkeypatch)
    handler = OHLCVDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        cache={"maxlen": 5},
        mode=EngineMode.BACKTEST,
        data_root=data_root,
    )
    handler.set_external_source(worker, emit=handler.on_new_tick)
    engine = _build_engine(
        handler,
        mode=EngineMode.BACKTEST,
        required_windows={"ohlcv": 3},
    )
    anchor_ts = 1_900_000_000_000  # outside fixture year, no local data
    engine.preload_data(anchor_ts=anchor_ts)

    with pytest.raises(RuntimeError, match="missing history"):
        engine.warmup_features(anchor_ts=anchor_ts)
    assert backfill_calls == []


def test_warmup_mock_triggers_backfill(tmp_path, monkeypatch) -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    worker, _raw_root, backfill_calls = _make_ohlcv_backfill_worker(tmp_path, monkeypatch)
    handler = OHLCVDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        cache={"maxlen": 5},
        mode=EngineMode.MOCK,
        data_root=data_root,
    )
    handler.set_external_source(worker, emit=handler.on_new_tick)
    engine = _build_engine(
        handler,
        mode=EngineMode.MOCK,
        required_windows={"ohlcv": 3},
    )
    anchor_ts = 1_900_000_000_000  # outside fixture year, no local data
    engine.preload_data(anchor_ts=anchor_ts)
    engine.warmup_features(anchor_ts=anchor_ts)

    assert backfill_calls == [
        (anchor_ts - 2 * INTERVAL_MS, anchor_ts),
    ]
    assert backfill_calls


@pytest.mark.asyncio
async def test_mock_driver_catchup_retriggers_backfill(tmp_path, monkeypatch) -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    worker, _raw_root, backfill_calls = _make_ohlcv_backfill_worker(tmp_path, monkeypatch)
    handler = OHLCVDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        cache={"maxlen": 5},
        mode=EngineMode.MOCK,
        data_root=data_root,
    )
    handler.set_external_source(worker, emit=handler.on_new_tick)

    class SpyExtractor:
        def __init__(self) -> None:
            self.required_windows = {"ohlcv": 1}
            self.warmup_steps = 1
            self.calls: list[int] = []

        def update(self, timestamp: int | None = None) -> dict:
            if timestamp is not None:
                self.calls.append(int(timestamp))
            return {}

        def warmup(self, *, anchor_ts: int) -> None:
            return None

        def set_interval(self, interval: str | None) -> None:
            return None

        def set_interval_ms(self, interval_ms: int | None) -> None:
            return None

    spec = EngineSpec.from_interval(mode=EngineMode.MOCK, interval="1m", symbol="BTCUSDT")
    execution_engine = ExecutionEngine(
        DummyPolicy(symbol="BTCUSDT"),
        DummyRouter(symbol="BTCUSDT"),
        DummySlippage(symbol="BTCUSDT"),
        DummyMatcher(symbol="BTCUSDT"),
    )
    extractor = SpyExtractor()
    engine = StrategyEngine(
        spec=spec,
        ohlcv_handlers={"BTCUSDT": handler},
        orderbook_handlers={},
        option_chain_handlers={},
        iv_surface_handlers={},
        sentiment_handlers={},
        trades_handlers={},
        option_trades_handlers={},
        feature_extractor=extractor,
        models={"main": DummyModel(symbol="BTCUSDT")},
        decision=DummyDecision(symbol="BTCUSDT"),
        risk_manager=DummyRisk(symbol="BTCUSDT"),
        execution_engine=execution_engine,
        portfolio_manager=DummyPortfolio(symbol="BTCUSDT"),
        guardrails=True,
    )

    anchor_ts = 1_900_000_000_000
    now_ts = anchor_ts + 2 * INTERVAL_MS
    driver = MockDriver(
        engine=engine,
        spec=spec,
        timestamps=[anchor_ts, anchor_ts + INTERVAL_MS, now_ts],
        ticks=[],
    )
    driver._catchup_now_ts = now_ts

    await driver.run()

    assert extractor.calls[:2] == [anchor_ts + INTERVAL_MS, now_ts]
    assert backfill_calls[-1] == (anchor_ts + INTERVAL_MS, now_ts)


def test_backtest_align_to_skips_backfill() -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    called = {"count": 0}

    class SpyOHLCVHandler(OHLCVDataHandler):
        def _maybe_backfill(self, *, target_ts: int) -> None:
            called["count"] += 1

    handler = SpyOHLCVHandler(
        symbol="BTCUSDT",
        interval="1m",
        cache={"maxlen": 3},
        mode=EngineMode.BACKTEST,
        data_root=data_root,
    )
    engine = _build_engine(handler, mode=EngineMode.BACKTEST)
    engine.align_to(BASE_TS)

    assert called["count"] == 0


def test_iv_surface_backfill_is_skipped() -> None:
    chain_handler = OptionChainDataHandler(
        symbol="BTCUSDT",
        interval="5m",
        mode=EngineMode.MOCK,
    )
    iv_handler = IVSurfaceDataHandler(
        symbol="BTCUSDT",
        interval="5m",
        chain_handler=chain_handler,
        mode=EngineMode.MOCK,
    )

    class SpySource:
        def __init__(self) -> None:
            self.calls = 0

        def backfill(self, *, start_ts: int, end_ts: int):
            self.calls += 1
            return []

        def persist(self, row: dict) -> None:
            return None

    source = SpySource()
    iv_handler.set_external_source(source)
    iv_handler._maybe_backfill(target_ts=BASE_TS)
    iv_handler._backfill_from_source(start_ts=BASE_TS, end_ts=BASE_TS, target_ts=BASE_TS)

    assert source.calls == 0
