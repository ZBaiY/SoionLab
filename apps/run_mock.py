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
from quant_engine.utils.logger import init_logging

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
    init_logging(run_id=_make_run_id())
    engine, driver_cfg, _ingestion_plan = build_mock_engine()
    driver = MockDriver(
        engine=engine,
        spec=engine.spec,
        timestamps=driver_cfg["timestamps"],
        ticks=driver_cfg["ticks"],
    )
    await driver.run()


if __name__ == "__main__":
    asyncio.run(main())
