from __future__ import annotations

import asyncio
import logging

import pytest

from ingestion.contracts.tick import IngestionTick
from quant_engine.contracts.portfolio import PortfolioState
from quant_engine.runtime.backtest import BacktestDriver
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.runtime.snapshot import EngineSnapshot


class _OptionChainHandler:
    def __init__(self, *, symbol: str = "BTCUSDT", last_ts: int | None = None) -> None:
        self.symbol = symbol
        self.interval_ms = 300_000
        self._last_ts = last_ts
        self.ready_checked = asyncio.Event()

    def last_timestamp(self) -> int | None:
        self.ready_checked.set()
        return self._last_ts

    def set_last_timestamp(self, ts: int | None) -> None:
        self._last_ts = None if ts is None else int(ts)


class _Engine:
    def __init__(self, handler: _OptionChainHandler, *, symbol: str = "BTCUSDT") -> None:
        self.symbol = symbol
        self.option_chain_handlers = {symbol: handler}
        self.ohlcv_handlers = {}
        self.step_calls = 0
        self.last_option_chain_ctx = None

    def load_history(self, *, start_ts: int, end_ts: int) -> None:
        return None

    def warmup_features(self, *, anchor_ts: int) -> None:
        return None

    def align_to(self, ts: int) -> None:
        return None

    def ingest_tick(self, tick: IngestionTick) -> None:
        if tick.domain == "option_chain":
            self.option_chain_handlers[self.symbol].set_last_timestamp(int(tick.data_ts))

    def step(self, *, ts: int) -> EngineSnapshot:
        self.step_calls += 1
        self.last_option_chain_ctx = getattr(self, "_step_option_chain_ctx", None)
        return EngineSnapshot(
            timestamp=int(ts),
            mode=EngineMode.BACKTEST,
            features={},
            model_outputs={},
            decision_score=0.0,
            target_position=0.0,
            fills=[],
            portfolio=PortfolioState({"symbol": self.symbol}),
        )

    def iter_shutdown_objects(self):
        return []


class _SingleStepBacktestDriver(BacktestDriver):
    async def iter_timestamps(self):
        yield 1_700_000_000_000


@pytest.mark.asyncio
async def test_backtest_option_chain_future_head_only_stops_wait_and_step_proceeds(
    caplog: pytest.LogCaptureFixture,
) -> None:
    step_ts = 1_700_000_000_000
    symbol = "ETHUSDT"
    handler = _OptionChainHandler(symbol=symbol, last_ts=None)
    engine = _Engine(handler, symbol=symbol)
    spec = EngineSpec.from_interval(
        mode=EngineMode.BACKTEST,
        interval="15m",
        symbol=symbol,
    )
    tick_queue: asyncio.PriorityQueue[tuple[int, int, IngestionTick]] = asyncio.PriorityQueue()
    future_ts = step_ts + 900_000
    await tick_queue.put(
        (
            future_ts,
            0,
            IngestionTick(
                timestamp=future_ts,
                data_ts=future_ts,
                domain="option_chain",
                symbol=symbol,
                payload={"data_ts": future_ts},
            ),
        )
    )
    admitted_ts = step_ts - 300_000

    async def _emit_current_tick() -> None:
        await handler.ready_checked.wait()
        await asyncio.sleep(0.02)
        await tick_queue.put(
            (
                admitted_ts,
                -1,
                IngestionTick(
                    timestamp=admitted_ts,
                    data_ts=admitted_ts,
                    domain="option_chain",
                    symbol=symbol,
                    payload={"data_ts": admitted_ts},
                ),
            )
        )

    producer = asyncio.create_task(_emit_current_tick())
    driver = _SingleStepBacktestDriver(
        engine=engine,  # type: ignore[arg-type]
        spec=spec,
        start_ts=step_ts,
        end_ts=step_ts,
        tick_queue=tick_queue,
        ingestion_tasks=[producer],
    )

    with caplog.at_level(logging.WARNING):
        await driver.run()

    assert engine.step_calls == 1
    assert isinstance(engine.last_option_chain_ctx, dict)
    assert engine.last_option_chain_ctx["admitted"] is True
    assert engine.last_option_chain_ctx["wait_state"] == "admitted"
    assert engine.last_option_chain_ctx["wait_rounds"] > 0

    warning_records = [rec for rec in caplog.records if rec.getMessage() == "backtest.option_chain.not_admitted"]
    assert len(warning_records) == 0


@pytest.mark.asyncio
async def test_backtest_option_chain_waits_before_warning_and_proceeds_when_exhausted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    step_ts = 1_700_000_000_000
    symbol = "LTCUSDT"
    handler = _OptionChainHandler(symbol=symbol, last_ts=None)
    engine = _Engine(handler, symbol=symbol)
    spec = EngineSpec.from_interval(
        mode=EngineMode.BACKTEST,
        interval="15m",
        symbol=symbol,
    )
    tick_queue: asyncio.PriorityQueue[tuple[int, int, IngestionTick]] = asyncio.PriorityQueue()
    future_ts = step_ts + 900_000
    await tick_queue.put(
        (
            future_ts,
            0,
            IngestionTick(
                timestamp=future_ts,
                data_ts=future_ts,
                domain="option_chain",
                symbol=symbol,
                payload={"data_ts": future_ts},
            ),
        )
    )

    async def _linger_then_finish() -> None:
        await handler.ready_checked.wait()
        await asyncio.sleep(0.02)

    producer = asyncio.create_task(_linger_then_finish())
    driver = _SingleStepBacktestDriver(
        engine=engine,  # type: ignore[arg-type]
        spec=spec,
        start_ts=step_ts,
        end_ts=step_ts,
        tick_queue=tick_queue,
        ingestion_tasks=[producer],
    )

    with caplog.at_level(logging.WARNING):
        await driver.run()

    assert engine.step_calls == 1
    assert isinstance(engine.last_option_chain_ctx, dict)
    assert engine.last_option_chain_ctx["admitted"] is False
    assert engine.last_option_chain_ctx["wait_state"] == "exhausted"
    assert engine.last_option_chain_ctx["wait_exhausted"] is True
    assert engine.last_option_chain_ctx["wait_rounds"] > 0
    assert engine.last_option_chain_ctx["procedural_stop"] == "future_head"

    warning_records = [rec for rec in caplog.records if rec.getMessage() == "backtest.option_chain.not_admitted"]
    assert len(warning_records) == 1
    context = getattr(warning_records[0], "context", {})
    assert context.get("wait_state") == "exhausted"
    assert context.get("wait_exhausted") is True
    assert int(context.get("wait_rounds")) > 0 # type: ignore[unreachable]
    assert context.get("procedural_stop") == "future_head"
    assert int(context.get("buffered_next_tick_ts")) == future_ts # type: ignore[unreachable]


@pytest.mark.asyncio
async def test_backtest_option_chain_admission_success_proceeds_without_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    step_ts = 1_700_000_000_000
    handler = _OptionChainHandler(last_ts=step_ts)
    engine = _Engine(handler)
    spec = EngineSpec.from_interval(
        mode=EngineMode.BACKTEST,
        interval="15m",
        symbol="BTCUSDT",
    )
    driver = _SingleStepBacktestDriver(
        engine=engine,  # type: ignore[arg-type]
        spec=spec,
        start_ts=step_ts,
        end_ts=step_ts,
        tick_queue=asyncio.PriorityQueue(),
    )

    with caplog.at_level(logging.WARNING):
        await driver.run()

    assert engine.step_calls == 1
    assert isinstance(engine.last_option_chain_ctx, dict)
    assert engine.last_option_chain_ctx["admitted"] is True
    assert engine.last_option_chain_ctx["wait_state"] == "admitted"
    warning_records = [rec for rec in caplog.records if rec.getMessage() == "backtest.option_chain.not_admitted"]
    assert len(warning_records) == 0


@pytest.mark.asyncio
async def test_backtest_option_chain_exhausted_wait_warns_and_proceeds_without_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    step_ts = 1_700_000_000_000
    symbol = "ETHUSDT"
    handler = _OptionChainHandler(symbol=symbol, last_ts=None)
    engine = _Engine(handler, symbol=symbol)
    spec = EngineSpec.from_interval(
        mode=EngineMode.BACKTEST,
        interval="15m",
        symbol=symbol,
    )
    tick_queue: asyncio.PriorityQueue[tuple[int, int, IngestionTick]] = asyncio.PriorityQueue()
    future_ts = step_ts + 900_000
    await tick_queue.put(
        (
            future_ts,
            0,
            IngestionTick(
                timestamp=future_ts,
                data_ts=future_ts,
                domain="option_chain",
                symbol=symbol,
                payload={"data_ts": future_ts},
            ),
        )
    )
    driver = _SingleStepBacktestDriver(
        engine=engine,  # type: ignore[arg-type]
        spec=spec,
        start_ts=step_ts,
        end_ts=step_ts,
        tick_queue=tick_queue,
    )

    with caplog.at_level(logging.WARNING):
        await driver.run()

    assert engine.step_calls == 1
    assert isinstance(engine.last_option_chain_ctx, dict)
    assert engine.last_option_chain_ctx["admitted"] is False
    assert engine.last_option_chain_ctx["wait_state"] == "exhausted"
    assert engine.last_option_chain_ctx["wait_exhausted"] is True
    assert engine.last_option_chain_ctx["procedural_stop"] == "future_head"

    warning_records = [rec for rec in caplog.records if rec.getMessage() == "backtest.option_chain.not_admitted"]
    assert len(warning_records) == 1
