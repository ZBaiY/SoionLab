from __future__ import annotations

import asyncio

import pytest

from ingestion.contracts.tick import IngestionTick
from quant_engine.contracts.portfolio import PortfolioState
from quant_engine.runtime.backtest import BacktestDriver
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.runtime.snapshot import EngineSnapshot


class _OhlcvHandler:
    def __init__(self, *, symbol: str = "BTCUSDT", interval_ms: int = 900_000) -> None:
        self.symbol = symbol
        self.interval_ms = int(interval_ms)
        self.interval = "15m"
        self._last_ts: int | None = None

    def last_timestamp(self) -> int | None:
        return self._last_ts

    def set_last_timestamp(self, ts: int | None) -> None:
        self._last_ts = None if ts is None else int(ts)


class _Engine:
    def __init__(self, handler: _OhlcvHandler, *, symbol: str = "BTCUSDT") -> None:
        self.symbol = symbol
        self.ohlcv_handlers = {symbol: handler}
        self.option_chain_handlers = {}
        self.orderbook_handlers = {}
        self.sentiment_handlers = {}
        self.iv_surface_handlers = {}
        self.trades_handlers = {}
        self.option_trades_handlers = {}
        self._last_tick_ts_by_key: dict[str, int] = {}
        self.step_calls = 0

    def load_history(self, *, start_ts: int, end_ts: int) -> None:
        return None

    def warmup_features(self, *, anchor_ts: int) -> None:
        return None

    def align_to(self, ts: int) -> None:
        return None

    def ingest_tick(self, tick: IngestionTick) -> None:
        handler = self.ohlcv_handlers[tick.symbol]
        handler.set_last_timestamp(int(tick.data_ts))
        self._last_tick_ts_by_key["ohlcv:BTCUSDT"] = int(tick.data_ts)

    def step(self, *, ts: int) -> EngineSnapshot:
        self.step_calls += 1
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
        yield 1_706_746_500_000


@pytest.mark.asyncio
async def test_gate_required_ohlcv_drains_pending_tick_before_missing_data() -> None:
    step_ts = 1_706_746_500_000
    need_ts = step_ts - 1
    symbol = "BTCUSDT"
    handler = _OhlcvHandler(symbol=symbol)
    engine = _Engine(handler, symbol=symbol)
    spec = EngineSpec.from_interval(mode=EngineMode.BACKTEST, interval="15m", symbol=symbol)
    tick_queue: asyncio.PriorityQueue[tuple[int, int, IngestionTick]] = asyncio.PriorityQueue()
    await tick_queue.put(
        (
            need_ts,
            0,
            IngestionTick(
                timestamp=need_ts,
                data_ts=need_ts,
                domain="ohlcv",
                symbol=symbol,
                payload={"data_ts": need_ts},
            ),
        )
    )
    driver = _SingleStepBacktestDriver(
        engine=engine,  # type: ignore[arg-type]
        spec=spec,
        start_ts=step_ts,
        end_ts=step_ts,
        tick_queue=tick_queue,
        ingestion_tasks=[],
    )

    await driver.run()

    assert engine.step_calls == 1
    assert handler.last_timestamp() == need_ts
