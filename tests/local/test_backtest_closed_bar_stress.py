from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from quant_engine.runtime.backtest import BacktestDriver
from quant_engine.utils.app_wiring import build_backtest_engine
from quant_engine.utils.cleaned_path_resolver import resolve_cleaned_paths
from quant_engine.utils.guards import ensure_epoch_ms
from quant_engine.utils.logger import init_logging
from quant_engine.utils.paths import data_root_from_file

START_TS = 1764374400000  # 2025-11-29 00:00:00 UTC 
END_TS = START_TS + 15 * 24 * 60 * 60 * 1000
RUNS = 3


def _trace_path(run_id: str) -> Path:
    return Path("artifacts") / "runs" / run_id / "logs" / "trace.jsonl"

def _step_ts(obj: dict) -> int:
    return int(obj.get("context", {}).get("timestamp") or obj.get("timestamp") or 0)

def _read_step_trace(path: Path, *, run_id: str) -> list[dict]:
    steps: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            rid = obj.get("context", {}).get("run_id") or obj.get("run_id")
            if rid != run_id:
                continue
            if obj.get("event") == "engine.step.trace":
                steps.append(obj)
    return steps


async def _run_once(*, run_id: str, data_root: Path) -> dict:
    init_logging(run_id=run_id)
    trace_path = _trace_path(run_id)
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    if trace_path.exists():
        trace_path.unlink()
    engine, driver_cfg, plan = build_backtest_engine(
        strategy_name="RSI-ADX-SIDEWAYS",
        bind_symbols={"A": "BTCUSDT", "window_RSI": "14", "window_ADX": "14", "window_RSI_rolling": "5"},
        start_ts=START_TS,
        end_ts=END_TS,
        data_root=data_root,
        require_local_data=True,
    )

    tick_queue: asyncio.PriorityQueue[tuple[int, int, object]] = asyncio.PriorityQueue(maxsize=4096)
    seq = 0

    async def emit_to_queue(tick: object) -> None:
        nonlocal seq
        ts = ensure_epoch_ms(getattr(tick, "data_ts", None))
        seq_key = seq
        seq += 1
        await tick_queue.put((int(ts), seq_key, tick))

    ingestion_tasks: list[asyncio.Task[None]] = []
    for entry in plan:
        if not entry.get("has_local_data"):
            pytest.skip("Missing local backtest data for stress run")
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

    steps = _read_step_trace(trace_path, run_id=run_id)
    first_not_ready_idx = None
    count_not_ready = 0
    first_not_ready_detail = None
    for idx, step in enumerate(steps, 1):
        closed_bar_ready = step.get("closed_bar_ready")
        if closed_bar_ready is False:
            count_not_ready += 1
            if first_not_ready_idx is None:
                first_not_ready_idx = idx
                expected_visible_end_ts = step.get("expected_visible_end_ts")
                actual_last_ts = step.get("actual_last_ts")
                delta_ms = None
                if expected_visible_end_ts is not None and actual_last_ts is not None:
                    delta_ms = int(expected_visible_end_ts) - int(actual_last_ts)
                handler = engine.ohlcv_handlers.get("BTCUSDT") or next(iter(engine.ohlcv_handlers.values()))
                first_not_ready_detail = {
                    "step_idx": idx,
                    "step_ts": _step_ts(step),
                    "expected_visible_end_ts": expected_visible_end_ts,
                    "actual_last_ts": actual_last_ts,
                    "delta_ms": delta_ms,
                    "queue_head_ts": None if driver.tick_queue is None or not getattr(driver.tick_queue, "_queue", None) else getattr(driver.tick_queue, "_queue")[0][0],
                    "queue_size": None if driver.tick_queue is None else driver.tick_queue.qsize(),
                    "symbol": getattr(handler, "symbol", None),
                    "interval_ms": getattr(handler, "interval_ms", None),
                }

    fingerprint = [
        (
            _step_ts(step),
            step.get("features"),
            step.get("market_snapshots"),
        )
        for step in steps
    ]
    step_map = {
        _step_ts(step): (step.get("features"), step.get("market_snapshots"))
        for step in steps
    }

    return {
        "steps": steps,
        "first_not_ready_step_idx": first_not_ready_idx,
        "count_not_ready_steps": count_not_ready,
        "first_not_ready_detail": first_not_ready_detail,
        "fingerprint": fingerprint,
        "step_map": step_map,
    }


@pytest.mark.local
@pytest.mark.asyncio
async def test_backtest_closed_bar_stress() -> None:
    data_root = data_root_from_file(__file__, levels_up=2)
    paths = resolve_cleaned_paths(
        data_root=data_root,
        domain="ohlcv",
        symbol="BTCUSDT",
        interval="15m",
        start_ts=START_TS,
        end_ts=END_TS,
    )
    if not any(p.exists() for p in paths):
        pytest.skip("No local OHLCV data for stress run")

    results: list[dict] = []
    base_fingerprint = None
    first_divergence_ts_list: list[int | None] = []
    divergence_detail_by_run: list[dict | None] = []
    for i in range(RUNS):
        run_id = f"local_backtest_stress_{i}"
        result = await _run_once(run_id=run_id, data_root=data_root)
        if base_fingerprint is None:
            base_fingerprint = result["fingerprint"]
            first_divergence_ts_list.append(None)
            divergence_detail_by_run.append(None)
        else:
            first_ts = None
            for (a_ts, a_feat, a_snap), (b_ts, b_feat, b_snap) in zip(base_fingerprint, result["fingerprint"]):
                if (a_ts, a_feat, a_snap) != (b_ts, b_feat, b_snap):
                    first_ts = a_ts
                    break
            first_divergence_ts_list.append(first_ts)
            if first_ts is not None:
                base_map = results[0]["step_map"]
                run_map = result["step_map"]
                divergence_detail_by_run.append(
                    {
                        "ts": first_ts,
                        "base_features": base_map.get(first_ts, (None, None))[0],
                        "base_snapshot": base_map.get(first_ts, (None, None))[1],
                        "run_features": run_map.get(first_ts, (None, None))[0],
                        "run_snapshot": run_map.get(first_ts, (None, None))[1],
                    }
                )
            else:
                divergence_detail_by_run.append(None)
        results.append(result)

    try:
        assert all(r["count_not_ready_steps"] == 0 for r in results)
    except AssertionError:
        for i, result in enumerate(results):
            run_id = f"local_backtest_stress_{i}"
            print(
                "run",
                run_id,
                "first_not_ready_step_idx",
                result["first_not_ready_step_idx"],
                "count_not_ready_steps",
                result["count_not_ready_steps"],
                "first_not_ready_detail",
                result["first_not_ready_detail"],
            )
        print("divergence_summary:", {f"local_backtest_stress_{i}": ts for i, ts in enumerate(first_divergence_ts_list)})
        for i, detail in enumerate(divergence_detail_by_run):
            if detail is None:
                continue
            run_id = f"local_backtest_stress_{i}"
            print("divergence_detail", run_id, detail)
        raise
