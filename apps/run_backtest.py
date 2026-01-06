from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from quant_engine.utils.logger import get_logger, init_logging, log_info, log_warn
from quant_engine.utils.guards import ensure_epoch_ms
from quant_engine.utils.paths import data_root_from_file
from quant_engine.runtime.backtest import BacktestDriver
from quant_engine.utils.apps_helpers import build_backtest_engine

STRATEGY_NAME = "EXAMPLE"
BIND_SYMBOLS = {"A": "BTCUSDT", "B": "ETHUSDT"}
# STRATEGY_NAME = "RSI-ADX-SIDEWAYS"
# BIND_SYMBOLS = {"A": "BTCUSDT", "window_RSI" : '14', "window_ADX": '14', "window_RSI_rolling": '5'}
START_TS = 1766966400000  # 2025-12-29 00:00:00 UTC (epoch ms)
END_TS = 1767052800000    # 2025-12-30 00:00:00 UTC (epoch ms)
DATA_ROOT = data_root_from_file(__file__, levels_up=1)


def _make_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _set_current_run(run_id: str) -> None:
    runs_dir = Path("artifacts") / "runs"
    target = runs_dir / run_id
    current = runs_dir / "_current"
    runs_dir.mkdir(parents=True, exist_ok=True)
    target.mkdir(parents=True, exist_ok=True)
    try:
        if current.exists() or current.is_symlink():
            current.unlink()
        current.symlink_to(target, target_is_directory=True)
    except (OSError, NotImplementedError):
        (runs_dir / "CURRENT").write_text(run_id, encoding="utf-8")


logger = get_logger(__name__)


async def main() -> None:
    run_id = _make_run_id()
    init_logging(run_id=run_id)
    _set_current_run(run_id)
    # -------------------------------------------------
    # 1. Load & bind strategy
    # -------------------------------------------------
    engine, driver_cfg, ingestion_plan = build_backtest_engine(
        strategy_name=STRATEGY_NAME,
        bind_symbols=BIND_SYMBOLS,
        start_ts=START_TS,
        end_ts=END_TS,
        data_root=DATA_ROOT,
    )

    log_info(
        logger,
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

    # -------------------------------------------------
    # 2. Build per-handler ingestion (generalized)
    # -------------------------------------------------
    # In backtest we stream ticks into the runtime (口径2):
    # ingestion runs fast (no throttling) and pushes ticks into a priority queue.
    tick_queue: asyncio.PriorityQueue[tuple[int, int, object]] = asyncio.PriorityQueue()
    _seq = 0

    ingestion_tasks: list[asyncio.Task[None]] = []

    async def emit_to_queue(tick: object) -> None:
        # Expect tick to have `.timestamp` (engine-time) attribute.
        nonlocal _seq
        ts = ensure_epoch_ms(getattr(tick, "data_ts"))
        await tick_queue.put((ts, _seq, tick))
        _seq += 1

    for entry in ingestion_plan:
        if not entry["has_local_data"]:
            log_info(
                logger,
                "ingestion.worker.skipped_no_data",
                domain=entry["domain"],
                symbol=entry["symbol"],
                root=entry["root"],
            )
            continue
        worker = entry["build_worker"]()
        log_info(
            logger,
            "ingestion.worker.start",
            domain=entry["domain"],
            symbol=entry["symbol"],
            source_type=entry["source_type"],
            has_local_data=entry["has_local_data"],
            start_ts=entry["start_ts"],
            end_ts=entry["end_ts"],
        )
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit_to_queue)))
    
    # -------------------------------------------------
    # 3. Run deterministic backtest (time only)
    # -------------------------------------------------
    driver = BacktestDriver(
        engine=engine,
        spec=engine.spec,
        start_ts=driver_cfg["start_ts"],
        end_ts=driver_cfg["end_ts"],
        tick_queue=tick_queue,
    )

    log_info(
        logger,
        "app.backtest.start",
        start_ts=driver_cfg["start_ts"],
        end_ts=driver_cfg["end_ts"],
    )

    await driver.run()
    
    log_info(logger, "ingestion.worker.stop", count=len(ingestion_tasks))
    for t in ingestion_tasks:
        t.cancel()
    if ingestion_tasks:
        await asyncio.gather(*ingestion_tasks, return_exceptions=True)

    # -------------------------------------------------
    # 4. Final snapshot / reports
    # -------------------------------------------------
    log_info(logger, "app.backtest.done")
    log_info(logger, "app.backtest.final_portfolio", portfolio=engine.portfolio.state().to_dict())


if __name__ == "__main__":
    asyncio.run(main())
