from __future__ import annotations

import asyncio
import time

import pytest

from ingestion.ohlcv.source import OHLCVRESTSource
from quant_engine.utils.asyncio import iter_source, to_thread_limited
from quant_engine.utils.logger import get_logger


@pytest.mark.asyncio
async def test_rest_source_fetch_timeout_and_cancel() -> None:
    logger = get_logger("tests.ingestion.rest_source")

    def slow_fetch() -> list[dict[str, int]]:
        time.sleep(0.05)
        return [{"data_ts": 1}]

    src = OHLCVRESTSource(fetch_fn=slow_fetch, poll_interval_ms=1)
    src._timeout = 0.01  # use iter_source timeout hook

    async def consume_once() -> None:
        async for _ in iter_source(
            src,
            logger=logger,
            context={"test": "rest_source"},
            poll_interval_s=None,
        ):
            return

    with pytest.raises(asyncio.TimeoutError):
        await consume_once()

    src._timeout = None
    task = asyncio.create_task(consume_once())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_rest_source_backfill_to_thread_timeout() -> None:
    logger = get_logger("tests.ingestion.rest_source")

    def slow_backfill(*, start_ts: int, end_ts: int) -> list[dict[str, int]]:
        time.sleep(0.05)
        return [{"data_ts": start_ts}, {"data_ts": end_ts}]

    src = OHLCVRESTSource(fetch_fn=lambda: [], backfill_fn=slow_backfill, poll_interval_ms=1)

    with pytest.raises(asyncio.TimeoutError):
        await to_thread_limited(
            src.backfill,
            start_ts=1,
            end_ts=2,
            logger=logger,
            op="rest_source_backfill",
            timeout_s=0.01,
        )
