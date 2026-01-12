from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Iterable

from ingestion.contracts.tick import IngestionTick
from quant_engine.runtime.mock import MockDriver
from quant_engine.runtime.modes import EngineMode
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.strategy.loader import StrategyLoader
from quant_engine.strategy.registry import get_strategy
from quant_engine.utils.logger import (
    init_logging,
    build_execution_constraints,
    build_trace_header,
    log_trace_header,
    get_logger,
)

DEFAULT_BIND_SYMBOLS = {"A": "BTCUSDT", "B": "ETHUSDT"}


def _make_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_mock_engine(
    *,
    strategy_name: str = "EXAMPLE",
    bind_symbols: dict[str, str] | None = None,
    overrides: dict | None = None,
    timestamps: Iterable[int] | None = None,
    ticks: Iterable[IngestionTick] | None = None,
) -> tuple[StrategyEngine, dict[str, Any], list[dict[str, Any]]]:
    StrategyCls = get_strategy(strategy_name)
    if bind_symbols is None:
        bind_symbols = dict(DEFAULT_BIND_SYMBOLS)
    cfg = StrategyCls.standardize(overrides=overrides or {}, symbols=bind_symbols)

    engine = StrategyLoader.from_config(
        strategy=cfg,
        mode=EngineMode.MOCK,
        overrides={},
    )
    driver_cfg = {
        "timestamps": [int(ts) for ts in (timestamps or [])],
        "ticks": list(ticks or []),
    }
    ingestion_plan: list[dict[str, Any]] = []
    return engine, driver_cfg, ingestion_plan


async def main() -> None:
    run_id = _make_run_id()
    init_logging(run_id=run_id)
    engine, driver_cfg, _ingestion_plan = build_mock_engine()
    logger = get_logger(__name__)
    timestamps = driver_cfg.get("timestamps") or []
    start_ts_ms = timestamps[0] if timestamps else None
    start_ts = None
    if start_ts_ms is not None:
        start_ts = datetime.fromtimestamp(start_ts_ms / 1000, tz=timezone.utc).isoformat()
    log_trace_header(
        logger,
        build_trace_header(
            run_id=run_id,
            engine_mode=engine.spec.mode.value,
            config_hash=getattr(engine, "config_hash", "unknown"),
            strategy_name=getattr(engine, "strategy_name", "unknown"),
            interval=engine.spec.interval,
            execution_constraints=build_execution_constraints(engine.portfolio),
            start_ts_ms=start_ts_ms,
            start_ts=start_ts,
        ),
    )
    driver = MockDriver(
        engine=engine,
        spec=engine.spec,
        timestamps=driver_cfg["timestamps"],
        ticks=driver_cfg["ticks"],
    )
    await driver.run()


if __name__ == "__main__":
    asyncio.run(main())
