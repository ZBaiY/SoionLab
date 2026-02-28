from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

import pytest

from apps.run_realtime import build_realtime_engine
from quant_engine.exceptions.core import FatalError
from quant_engine.runtime.realtime import RealtimeDriver

pytestmark = [pytest.mark.live_api, pytest.mark.integration, pytest.mark.slow]


def _set_handler_data_roots(engine, data_root: Path) -> None:
    for h in engine.ohlcv_handlers.values():
        h._data_root = data_root
    for h in engine.option_chain_handlers.values():
        h._data_root = data_root


@pytest.mark.timeout(120)
@pytest.mark.asyncio
async def test_live_api_bootstrap_and_warmup_smoke(tmp_data_root: Path, caplog: pytest.LogCaptureFixture) -> None:
    engine, _, _ = build_realtime_engine(strategy_name="EXAMPLE", bind_symbols={"A": "BTCUSDT"})
    _set_handler_data_roots(engine, tmp_data_root)

    anchor_ts = int(time.time() * 1000)
    with caplog.at_level(logging.WARNING):
        engine.bootstrap(anchor_ts=anchor_ts)
        engine.warmup_features(anchor_ts=anchor_ts)

    ohlcv_h = next(iter(engine.ohlcv_handlers.values()))
    last_ohlcv_ts = ohlcv_h.last_timestamp()
    assert last_ohlcv_ts is not None
    assert int(last_ohlcv_ts) >= anchor_ts - (2 * int(ohlcv_h.interval_ms))

    if engine.option_chain_handlers:
        option_h = next(iter(engine.option_chain_handlers.values()))
        option_ts = option_h.last_timestamp()
        if option_ts is None:
            assert any("engine.warmup.soft_domain_insufficient" in rec.getMessage() for rec in caplog.records)


@pytest.mark.timeout(120)
@pytest.mark.asyncio
async def test_live_api_ingestion_loop_advances_and_persists(
    tmp_data_root: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    engine, _, ingestion_plan = build_realtime_engine(strategy_name="EXAMPLE", bind_symbols={"A": "BTCUSDT"})
    _set_handler_data_roots(engine, tmp_data_root)

    ingestion_tasks: list[asyncio.Task[None]] = []
    for entry in ingestion_plan:
        worker = entry["build_worker"]()
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=entry["emit"])))

    driver = RealtimeDriver(engine=engine, spec=engine.spec)
    step_count = {"n": 0}
    original_step = engine.step

    def _counting_step(*, ts: int):
        result = original_step(ts=ts)
        step_count["n"] += 1
        if step_count["n"] >= 2:
            driver.stop_event.set()
        return result

    engine.step = _counting_step  # type: ignore[method-assign]

    try:
        with caplog.at_level(logging.WARNING):
            await driver.run()
    except FatalError:
        raise
    finally:
        for t in ingestion_tasks:
            t.cancel()
        if ingestion_tasks:
            await asyncio.gather(*ingestion_tasks, return_exceptions=True)

    assert step_count["n"] >= 1
    assert len(driver.snapshots) >= 1
    assert any((tmp_data_root / "raw" / "ohlcv").rglob("*.parquet"))


@pytest.mark.timeout(90)
def test_live_api_gap_handling_policy_smoke(tmp_data_root: Path, caplog: pytest.LogCaptureFixture) -> None:
    engine, _, _ = build_realtime_engine(strategy_name="EXAMPLE", bind_symbols={"A": "BTCUSDT"})
    _set_handler_data_roots(engine, tmp_data_root)

    anchor_ts = int(time.time() * 1000)
    engine.bootstrap(anchor_ts=anchor_ts)

    ohlcv_h = next(iter(engine.ohlcv_handlers.values()))
    if hasattr(ohlcv_h, "cache") and hasattr(ohlcv_h.cache, "buffer"):
        ohlcv_h.cache.buffer.clear()

    with caplog.at_level(logging.WARNING):
        engine.align_to(anchor_ts + int(ohlcv_h.interval_ms))

    assert ohlcv_h._should_backfill() is True
    for option_h in engine.option_chain_handlers.values():
        assert option_h._should_backfill() is False
