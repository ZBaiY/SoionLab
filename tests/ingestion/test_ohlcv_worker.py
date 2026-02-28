from __future__ import annotations

import asyncio

import pytest

from ingestion.contracts.tick import IngestionTick
from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer
from ingestion.ohlcv.worker import OHLCVWorker
from ingestion.contracts.tick import _to_interval_ms


@pytest.mark.asyncio
async def test_ohlcv_worker_run_emits_ticks_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {
            "open_time": 1000,
            "close_time": 1500,
            "open": "1.0",
            "high": "1.1",
            "low": "0.9",
            "close": "1.05",
            "volume": "10",
        },
        {
            "open_time": 2000,
            "close_time": 2500,
            "open": "1.1",
            "high": "1.2",
            "low": "1.0",
            "close": "1.15",
            "volume": "12",
        },
    ]

    class _StaticSource:
        def __iter__(self):
            for r in rows:
                yield r

    async def fast_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    source = _StaticSource()
    normalizer = BinanceOHLCVNormalizer(symbol="BTCUSDT")
    interval_ms = _to_interval_ms("1m")
    worker = OHLCVWorker(
        normalizer=normalizer,
        source=source,  # type: ignore[arg-type]
        symbol="BTCUSDT",
        interval="1m",
        interval_ms=int(interval_ms) if interval_ms is not None else None,
        poll_interval_ms=1,
    )

    emitted: list[IngestionTick] = []

    async def emit(tick: IngestionTick) -> None:
        emitted.append(tick)

    await worker.run(emit)

    assert [tick.data_ts for tick in emitted] == [1500_000, 2500_000]
    assert all(isinstance(tick, IngestionTick) for tick in emitted)
