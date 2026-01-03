from __future__ import annotations

import asyncio

import pytest

from ingestion.contracts.tick import IngestionTick
from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer
from ingestion.ohlcv.source import OHLCVFileSource
from ingestion.ohlcv.worker import OHLCVWorker
from quant_engine.runtime.modes import EngineMode
from quant_engine.strategy.registry import get_strategy
from quant_engine.utils.paths import data_root_from_file

from tests.integration.helpers import earliest_ohlcv_ts_ms, find_ohlcv_root

START_TS = 1622505600000  # June 1, 2021
# END_TS = START_TS + 60000  # One minute later

@pytest.mark.integration
@pytest.mark.local
@pytest.mark.asyncio
async def test_realtime_wiring_smoke_with_file_source() -> None:
    data_root = data_root_from_file(__file__, levels_up=3)
    StrategyCls = get_strategy("EXAMPLE")
    strategy = StrategyCls().bind(A="BTCUSDT", B="ETHUSDT")
    engine = strategy.build(mode=EngineMode.REALTIME)

    ingestion_tasks: list[asyncio.Task[None]] = []
    snapshot = None
    for symbol, handler in engine.ohlcv_handlers.items():
        interval = getattr(handler, "interval", None)
        if not interval:
            continue
        root = find_ohlcv_root(data_root, symbol, str(interval))
        if root is None:
            continue
        base = root / symbol / str(interval)
        start_ts = START_TS
        if start_ts is None:
            continue
        end_ts = start_ts + int(engine.spec.interval_ms)

        handler.align_to(end_ts)

        source = OHLCVFileSource(
            root=root,
            symbol=symbol,
            interval=str(interval),
            start_ts=start_ts,
            end_ts=end_ts,
        )
        normalizer = BinanceOHLCVNormalizer(symbol=symbol)
        worker = OHLCVWorker(
            source=source,
            normalizer=normalizer,
            symbol=symbol,
            poll_interval=None,
        )

        async def emit(tick: IngestionTick) -> None:
            handler.on_new_tick(tick)

        ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit)))

    if not ingestion_tasks:
        pytest.skip("No local OHLCV parquet data available for realtime wiring smoke test")

    try:
        await asyncio.gather(*ingestion_tasks)
    finally:
        for task in ingestion_tasks:
            task.cancel()
        await asyncio.gather(*ingestion_tasks, return_exceptions=True)

    for symbol, handler in engine.ohlcv_handlers.items():
        snap = handler.get_snapshot()
        if snap is not None:
            snapshot = snap
            break

    assert snapshot is not None
