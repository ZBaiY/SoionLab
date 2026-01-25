from __future__ import annotations

import asyncio
from pathlib import Path

import pandas as pd
import pytest

from quant_engine.runtime.backtest import BacktestDriver
from quant_engine.utils.app_wiring import build_backtest_engine
from quant_engine.utils.cleaned_path_resolver import resolve_cleaned_paths
from quant_engine.utils.guards import ensure_epoch_ms
from quant_engine.utils.paths import data_root_from_file

TARGET_TS = 1764686700000  # 2025-12-02 14:45:00 UTC
INTERVAL_MS = 15 * 60 * 1000
LOOKBACK_BARS = 400


def _min_max_data_ts(paths: list[Path]) -> tuple[int | None, int | None]:
    mins: list[int] = []
    maxs: list[int] = []
    for path in paths:
        if not path.exists():
            continue
        df = pd.read_parquet(path, columns=["data_ts"])
        if df.empty:
            continue
        mins.append(int(df["data_ts"].min()))
        maxs.append(int(df["data_ts"].max()))
    if not mins or not maxs:
        return None, None
    return min(mins), max(maxs)


async def _run_once(*, start_ts: int, end_ts: int, data_root: Path) -> tuple[list[int], list[int | None], int]:
    engine, driver_cfg, plan = build_backtest_engine(
        strategy_name="RSI-ADX-SIDEWAYS",
        bind_symbols={"A": "BTCUSDT", "window_RSI": "14", "window_ADX": "14", "window_RSI_rolling": "5"},
        start_ts=start_ts,
        end_ts=end_ts,
        data_root=data_root,
        require_local_data=True,
    )

    tick_queue: asyncio.PriorityQueue[tuple[int, int, object]] = asyncio.PriorityQueue(maxsize=1024)
    seq = 0

    async def emit_to_queue(tick: object) -> None:
        nonlocal seq
        ts = ensure_epoch_ms(getattr(tick, "data_ts", None))
        seq_key = seq
        seq += 1
        await tick_queue.put((int(ts), -seq_key, tick))

    ingestion_tasks: list[asyncio.Task[None]] = []
    for entry in plan:
        if not entry.get("has_local_data"):
            pytest.skip("Missing local backtest data for deterministic replay")
        worker = entry["build_worker"]()
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit_to_queue)))

    driver = BacktestDriver(
        engine=engine,
        spec=engine.spec,
        start_ts=driver_cfg["start_ts"],
        end_ts=driver_cfg["end_ts"],
        tick_queue=tick_queue,
        ingestion_tasks=ingestion_tasks,
    )

    try:
        await asyncio.gather(driver.run(), *ingestion_tasks)
    finally:
        for task in ingestion_tasks:
            task.cancel()
        if ingestion_tasks:
            await asyncio.gather(*ingestion_tasks, return_exceptions=True)

    assert driver.snapshots
    ts_list = [int(snap.timestamp) for snap in driver.snapshots]
    ohlcv_handler = engine.ohlcv_handlers.get("BTCUSDT") or next(iter(engine.ohlcv_handlers.values()))
    data_ts_list: list[int | None] = []
    for ts in ts_list:
        snap = ohlcv_handler.get_snapshot(ts)
        data_ts_list.append(None if snap is None else int(snap.data_ts))
    interval_ms = int(getattr(ohlcv_handler, "interval_ms"))
    return ts_list, data_ts_list, interval_ms


@pytest.mark.integration
@pytest.mark.local
@pytest.mark.asyncio
async def test_backtest_closed_bar_visibility_is_deterministic() -> None:
    data_root = data_root_from_file(__file__, levels_up=2)
    start_ts = int(TARGET_TS - LOOKBACK_BARS * INTERVAL_MS)
    end_ts = int(TARGET_TS + 2 * INTERVAL_MS)

    paths = resolve_cleaned_paths(
        data_root=data_root,
        domain="ohlcv",
        symbol="BTCUSDT",
        interval="15m",
        start_ts=start_ts,
        end_ts=end_ts,
    )
    if not any(p.exists() for p in paths):
        pytest.skip("No local OHLCV parquet data available for deterministic backtest")

    min_ts, max_ts = _min_max_data_ts(paths)
    if min_ts is None or max_ts is None:
        pytest.skip("No OHLCV data_ts values found for deterministic backtest")

    required_history_start = start_ts - (321 - 1) * INTERVAL_MS
    if required_history_start < min_ts or TARGET_TS > max_ts:
        pytest.skip("Insufficient OHLCV history around target timestamp for deterministic backtest")

    ts_a, data_ts_a, interval_ms = await _run_once(start_ts=start_ts, end_ts=end_ts, data_root=data_root)
    ts_b, data_ts_b, _ = await _run_once(start_ts=start_ts, end_ts=end_ts, data_root=data_root)

    assert ts_a == ts_b
    assert data_ts_a == data_ts_b

    for ts, data_ts in zip(ts_a, data_ts_a):
        assert data_ts is not None
        visible_end_ts = (int(ts) // int(interval_ms)) * int(interval_ms) - 1
        assert int(data_ts) <= int(visible_end_ts)

    if TARGET_TS in ts_a:
        idx = ts_a.index(TARGET_TS)
        visible_end_ts = (TARGET_TS // interval_ms) * interval_ms - 1
        assert data_ts_a[idx] == visible_end_ts
