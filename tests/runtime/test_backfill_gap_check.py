from __future__ import annotations

from pathlib import Path
import logging

import pytest

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
from quant_engine.utils.num import visible_end_ts

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
    option_chain_handlers: dict[str, OptionChainDataHandler] | None = None,
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
        option_chain_handlers=option_chain_handlers or {},
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


def _make_tick(ts: int, *, source_id: str | None = None) -> IngestionTick:
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
        source_id=source_id,
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
                    "data_ts": int(ts), # type: ignore[dict-item]
                    "open_time": int(ts - INTERVAL_MS),
                    "close_time": int(ts),
                    "open": "1.0",
                    "high": "1.0",
                    "low": "1.0",
                    "close": "1.0",
                    "volume": "1.0",
                }
            ) # type: ignore[list-item]
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
        interval_ms=INTERVAL_MS,
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
    # Closed-bar semantics: at exact anchor boundary, visible_end_ts excludes anchor bar.
    assert [s.data_ts for s in snaps] == [
        BASE_TS + 2 * INTERVAL_MS,
        BASE_TS + 3 * INTERVAL_MS,
    ]


def test_gap_backfill_in_mock_mode(tmp_path, monkeypatch) -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    worker, _raw_root, backfill_calls = _make_ohlcv_backfill_worker(tmp_path, monkeypatch)
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
    handler.on_new_tick(_make_tick(seed_ts, source_id=handler.source_id))

    target_ts = BASE_TS + 5 * INTERVAL_MS
    engine.align_to(target_ts)

    last_ts = handler.last_timestamp()
    assert last_ts is not None
    assert int(last_ts) >= int(target_ts) - int(handler.interval_ms)
    assert backfill_calls == [
        (seed_ts + INTERVAL_MS, target_ts - 1),
    ]

    ordered = [s.data_ts for s in handler.cache.window(10)]
    assert ordered == sorted(ordered)


def test_gap_backfill_does_not_trigger_on_exact_closed_bar_boundary(
    tmp_path,
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    worker, _raw_root, backfill_calls = _make_ohlcv_backfill_worker(tmp_path, monkeypatch)
    handler = OHLCVDataHandler(
        symbol="BTCUSDT",
        interval="15m",
        cache={"maxlen": 10},
        mode=EngineMode.MOCK,
        data_root=data_root,
    )
    handler.set_external_source(worker, emit=handler.on_new_tick)
    closed_bar_ts = 1_773_144_899_999
    handler.align_to(closed_bar_ts)
    handler.on_new_tick(
        _make_tick(closed_bar_ts, source_id=handler.source_id)
    )

    boundary_ts = closed_bar_ts + 1
    with caplog.at_level(logging.WARNING):
        handler._maybe_backfill(target_ts=boundary_ts)

    assert backfill_calls == []
    assert not any("ohlcv.gap_detected" in rec.getMessage() for rec in caplog.records)


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


def test_warmup_realtime_logs_missing_history_warning_and_backfill_start_info(
    tmp_path,
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    worker, _raw_root, _backfill_calls = _make_ohlcv_backfill_worker(tmp_path, monkeypatch)
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
    anchor_ts = 1_900_000_000_000
    engine.preload_data(anchor_ts=anchor_ts)

    with caplog.at_level(logging.INFO):
        engine.warmup_features(anchor_ts=anchor_ts)

    missing_records = [rec for rec in caplog.records if "engine.warmup.missing_history" in rec.getMessage()]
    backfill_records = [rec for rec in caplog.records if "engine.warmup.backfill.start" in rec.getMessage()]
    assert missing_records
    assert backfill_records
    assert missing_records[0].levelno == logging.WARNING
    assert backfill_records[0].levelno == logging.INFO


def test_preload_logs_partial_fill_info_on_cold_start(caplog: pytest.LogCaptureFixture) -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    handler = OHLCVDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        cache={"maxlen": 50},
        mode=EngineMode.MOCK,
        data_root=data_root,
    )
    engine = _build_engine(
        handler,
        mode=EngineMode.MOCK,
        required_windows={"ohlcv": 5},
    )
    anchor_ts = 1_900_000_000_000  # outside fixture year, preload returns no bars
    with caplog.at_level(logging.INFO):
        engine.preload_data(anchor_ts=anchor_ts)

    records = [rec for rec in caplog.records if "engine.preload.partial_fill" in rec.getMessage()]
    assert records
    assert records[0].levelno == logging.INFO


def test_warmup_mixed_domains_option_chain_soft_in_mock(caplog: pytest.LogCaptureFixture) -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    ohlcv_handler = OHLCVDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        cache={"maxlen": 50},
        mode=EngineMode.MOCK,
        data_root=data_root,
    )
    option_handler = OptionChainDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        mode=EngineMode.MOCK,
        data_root=data_root,
        preset="option_chain",
    )
    engine = _build_engine(
        ohlcv_handler,
        mode=EngineMode.MOCK,
        required_windows={"ohlcv": 2, "option_chain": 5},
        option_chain_handlers={"BTCUSDT": option_handler},
    )
    anchor_ts = BASE_TS + 2 * INTERVAL_MS
    engine.preload_data(anchor_ts=anchor_ts)
    with caplog.at_level(logging.WARNING):
        engine.warmup_features(anchor_ts=anchor_ts)

    assert any("engine.warmup.soft_domain_insufficient" in rec.getMessage() for rec in caplog.records)


def test_warmup_mixed_domains_option_chain_soft_in_backtest(caplog: pytest.LogCaptureFixture) -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    ohlcv_handler = OHLCVDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        cache={"maxlen": 50},
        mode=EngineMode.BACKTEST,
        data_root=data_root,
    )
    option_handler = OptionChainDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        mode=EngineMode.BACKTEST,
        data_root=data_root,
        preset="option_chain",
    )
    engine = _build_engine(
        ohlcv_handler,
        mode=EngineMode.BACKTEST,
        required_windows={"ohlcv": 2, "option_chain": 5},
        option_chain_handlers={"BTCUSDT": option_handler},
    )
    anchor_ts = BASE_TS + 2 * INTERVAL_MS
    engine.preload_data(anchor_ts=anchor_ts)
    with caplog.at_level(logging.WARNING):
        engine.warmup_features(anchor_ts=anchor_ts)

    assert any("engine.warmup.soft_domain_insufficient" in rec.getMessage() for rec in caplog.records)


def test_align_to_backfill_once_per_timestamp() -> None:
    data_root = Path(__file__).resolve().parents[1] / "resources"
    handler = OHLCVDataHandler(
        symbol="BTCUSDT",
        interval="1m",
        cache={"maxlen": 10},
        mode=EngineMode.MOCK,
        data_root=data_root,
    )
    engine = _build_engine(handler, mode=EngineMode.MOCK, required_windows={"ohlcv": 1})
    calls = {"n": 0}

    def _count_backfill(*, target_ts: int) -> None:
        calls["n"] += 1

    handler._maybe_backfill = _count_backfill  # type: ignore[attr-defined]
    ts = BASE_TS + 5 * INTERVAL_MS
    engine.align_to(ts)
    engine.align_to(ts)
    engine.align_to(ts + INTERVAL_MS)
    assert calls["n"] == 2


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
        (anchor_ts - 3 * INTERVAL_MS, anchor_ts),
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
    driver._catchup_now_ts = now_ts # type: ignore[attr-defined]

    await driver.run()

    assert backfill_calls[0] == (anchor_ts - INTERVAL_MS, anchor_ts)
    assert backfill_calls[1] == (anchor_ts + INTERVAL_MS, visible_end_ts(now_ts, INTERVAL_MS))


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
        preset="option_chain",
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
