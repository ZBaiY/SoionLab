from __future__ import annotations

import asyncio

import pytest

from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer
from ingestion.ohlcv.source import OHLCVFileSource
from ingestion.ohlcv.worker import OHLCVWorker
from quant_engine.runtime.backtest import BacktestDriver
from quant_engine.runtime.modes import EngineMode
from quant_engine.strategy.registry import get_strategy
from quant_engine.utils.guards import ensure_epoch_ms
from quant_engine.utils.paths import data_root_from_file

from tests.integration.helpers import earliest_ohlcv_ts_ms, find_ohlcv_root

START_TS = 1622505600000  # June 1, 2021

@pytest.mark.integration
@pytest.mark.local
@pytest.mark.asyncio
async def test_strategy_engine_backtest_with_real_ohlcv() -> None:
    data_root = data_root_from_file(__file__, levels_up=3)
    StrategyCls = get_strategy("EXAMPLE")
    strategy = StrategyCls().bind(A="BTCUSDT", B="ETHUSDT")
    engine = strategy.build(mode=EngineMode.BACKTEST)

    tick_queue: asyncio.PriorityQueue[tuple[int, int, object]] = asyncio.PriorityQueue(maxsize=1024)
    seq = 0

    async def emit_to_queue(tick: object) -> None:
        nonlocal seq
        ts = ensure_epoch_ms(getattr(tick, "timestamp"))
        await tick_queue.put((int(ts), seq, tick))
        seq += 1

    start_ts: int | None = None
    ingestion_tasks: list[asyncio.Task[None]] = []

    for symbol, handler in engine.ohlcv_handlers.items():
        interval = getattr(handler, "interval", None)
        if not interval:
            continue
        root = find_ohlcv_root(data_root, symbol, str(interval))
        if root is None:
            continue
        base = root / symbol / str(interval)
        # start_ts = earliest_ohlcv_ts_ms(base) if start_ts is None else start_ts
        start_ts = START_TS
        if start_ts is None:
            continue
        source = OHLCVFileSource(
            root=root,
            symbol=symbol,
            interval=str(interval),
            start_ts=start_ts,
            end_ts=start_ts + 2 * int(engine.spec.interval_ms),
        )
        normalizer = BinanceOHLCVNormalizer(symbol=symbol)
        worker = OHLCVWorker(
            source=source,
            normalizer=normalizer,
            symbol=symbol,
            poll_interval=None,
        )
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit_to_queue)))

    if not ingestion_tasks or start_ts is None:
        pytest.skip("No local OHLCV parquet data available for backtest")

    driver = BacktestDriver(
        engine=engine,
        spec=engine.spec,
        start_ts=int(start_ts),
        end_ts=int(start_ts + 2 * int(engine.spec.interval_ms)),
        tick_queue=tick_queue,
    )

    try:
        await asyncio.gather(driver.run(), *ingestion_tasks)
    finally:
        for task in ingestion_tasks:
            task.cancel()
        await asyncio.gather(*ingestion_tasks, return_exceptions=True)

    assert driver.snapshots
    last_snapshot = driver.snapshots[-1]
    assert hasattr(last_snapshot, "timestamp")
    assert hasattr(last_snapshot, "portfolio")
    assert last_snapshot.portfolio is not None
