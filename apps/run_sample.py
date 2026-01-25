from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from apps.run_code.backtest_app import _make_run_id, _set_current_run
from quant_engine.runtime.backtest import BacktestDriver
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.strategy.registry import get_strategy
from quant_engine.utils.app_wiring import build_backtest_engine
from quant_engine.utils.guards import ensure_epoch_ms
from quant_engine.utils.logger import (
    build_execution_constraints,
    build_trace_header,
    get_logger,
    init_logging,
    log_info,
    log_trace_header,
)
from quant_engine.utils.paths import data_root_from_file, repo_root_from_file, set_engine_spec_context

STRATEGY_NAME = "EXAMPLE"
BIND_SYMBOLS = {"A": "BTCUSDT", "B": "ETHUSDT"}

START_TS = 1766966400000 - 10 * 24 * 60 * 60 * 1000  # 2025-11-29 00:00:00 UTC (epoch ms)
END_TS = 1767052800000     # 2025-12-30 00:00:00 UTC (epoch ms) + 3 hours buffer


_LOGGER = get_logger(__name__)
def _load_sample_presets() -> dict:
    repo_root = repo_root_from_file(__file__, levels_up=1)
    path = repo_root / "configs" / "refs_sample.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _sample_overrides() -> dict:
    strategy_cls = get_strategy(STRATEGY_NAME)
    bound_spec = strategy_cls.bind_spec(symbols=BIND_SYMBOLS)
    universe = bound_spec.get("universe") or {}
    if not isinstance(universe, dict):
        universe = {}
    soft_cfg = universe.get("soft_readiness")
    if not isinstance(soft_cfg, dict):
        soft_cfg = {}
    soft_cfg = dict(soft_cfg)
    soft_cfg["enabled"] = True
    soft_cfg.setdefault(
        "domains",
        ["orderbook", "option_chain", "iv_surface", "sentiment", "trades", "option_trades"],
    )
    soft_cfg.setdefault("max_staleness_ms", 300000) # current lack of data should not block execution for 5 minutes
    universe["soft_readiness"] = soft_cfg
    return {"universe": universe, "presets": _load_sample_presets()}


async def main() -> None:
    strategy_cls = get_strategy(STRATEGY_NAME)
    overrides = _sample_overrides()
    cfg = strategy_cls.standardize(overrides=overrides, symbols=BIND_SYMBOLS)
    interval_ms = cfg.interval_ms if cfg.interval_ms is not None else 0
    if not isinstance(interval_ms, int) or interval_ms <= 0:
        raise ValueError(f"Invalid interval_ms in sample config: {cfg.interval_ms!r}")
    engine_spec = EngineSpec(
        mode=EngineMode.SAMPLE,
        interval=cfg.interval,
        interval_ms=interval_ms,
        symbol=str(cfg.symbol),
        universe=cfg.universe,
    )
    set_engine_spec_context(engine_spec)
    data_root = data_root_from_file(__file__, levels_up=1)

    run_id = _make_run_id(STRATEGY_NAME)
    init_logging(run_id=run_id)
    _set_current_run(run_id)

    engine, driver_cfg, ingestion_plan = build_backtest_engine(
        strategy_name=STRATEGY_NAME,
        bind_symbols=BIND_SYMBOLS,
        start_ts=START_TS,
        end_ts=END_TS,
        data_root=data_root,
        require_local_data=True,
        overrides=overrides,
        engine_spec=engine_spec,
    )
    log_trace_header(
        _LOGGER,
        build_trace_header(
            run_id=run_id,
            engine_mode=engine.spec.mode.value,
            config_hash=getattr(engine, "config_hash", "unknown"),
            strategy_name=getattr(engine, "strategy_name", "unknown"),
            interval=engine.spec.interval,
            execution_constraints=build_execution_constraints(engine.portfolio),
            start_ts_ms=driver_cfg.get("start_ts"),
            start_ts=(
                datetime.fromtimestamp(driver_cfg["start_ts"] / 1000, tz=timezone.utc).isoformat()
                if driver_cfg.get("start_ts") is not None
                else None
            ),
        ),
    )

    log_info(
        _LOGGER,
        "app.engine.built",
        mode=engine.spec.mode.value,
        interval=engine.spec.interval,
        symbols=list(engine.universe.values()) if engine.universe else [],
        domains=[
            d
            for d, h in (
                ("ohlcv", engine.ohlcv_handlers),
                ("orderbook", engine.orderbook_handlers),
                ("option_chain", engine.option_chain_handlers),
                ("iv_surface", engine.iv_surface_handlers),
                ("sentiment", engine.sentiment_handlers),
                ("trades", engine.trades_handlers),
                ("option_trades", engine.option_trades_handlers),
            )
            if h
        ],
    )

    tick_queue: asyncio.PriorityQueue[tuple[int, int, object]] = asyncio.PriorityQueue()
    _seq = 0
    ingestion_tasks: list[asyncio.Task[None]] = []

    async def emit_to_queue(tick: object) -> None:
        nonlocal _seq
        ts = ensure_epoch_ms(getattr(tick, "data_ts"))
        seq_key = _seq
        _seq += 1
        await tick_queue.put((ts, -seq_key, tick))

    for entry in ingestion_plan:
        if not entry["has_local_data"]:
            log_info(
                _LOGGER,
                "ingestion.worker.skipped_no_data",
                domain=entry["domain"],
                symbol=entry["symbol"],
                root=entry["root"],
            )
            continue
        worker = entry["build_worker"]()
        log_info(
            _LOGGER,
            "ingestion.worker.start",
            domain=entry["domain"],
            symbol=entry["symbol"],
            source_type=entry["source_type"],
            has_local_data=entry["has_local_data"],
            start_ts=entry["start_ts"],
            end_ts=entry["end_ts"],
        )
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit_to_queue)))

    driver = BacktestDriver(
        engine=engine,
        spec=engine.spec,
        start_ts=driver_cfg["start_ts"],
        end_ts=driver_cfg["end_ts"],
        tick_queue=tick_queue,
        ingestion_tasks=ingestion_tasks,
    )
    log_info(
        _LOGGER,
        "app.backtest.start",
        start_ts=driver_cfg["start_ts"],
        end_ts=driver_cfg["end_ts"],
    )

    await driver.run()

    log_info(_LOGGER, "ingestion.worker.stop", count=len(ingestion_tasks))
    for t in ingestion_tasks:
        t.cancel()
    if ingestion_tasks:
        await asyncio.gather(*ingestion_tasks, return_exceptions=True)

    log_info(_LOGGER, "app.backtest.done")
    log_info(_LOGGER, "app.backtest.final_portfolio", portfolio=engine.portfolio.state().to_dict())


if __name__ == "__main__":
    asyncio.run(main())
