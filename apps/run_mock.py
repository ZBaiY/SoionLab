from __future__ import annotations

import asyncio
import argparse
from datetime import datetime, timezone
from typing import Any, Iterable

from apps.run_code.backtest_app import _set_current_run
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


def _parse_bind_symbols(text: str) -> dict[str, str]:
    pairs = [part.strip() for part in str(text).split(",") if part.strip()]
    if not pairs:
        raise ValueError("bind symbols must not be empty")
    out: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"invalid bind symbol pair: {pair!r}; expected KEY=VALUE")
        k, v = pair.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k or not v:
            raise ValueError(f"invalid bind symbol pair: {pair!r}; expected KEY=VALUE")
        out[k] = v
    return out


def _parse_timestamps(text: str | None) -> list[int]:
    if text is None or not str(text).strip():
        return []
    values = [part.strip() for part in str(text).split(",") if part.strip()]
    return [int(v) for v in values]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run mock app")
    parser.add_argument("--strategy", default="EXAMPLE", help="strategy name in registry")
    parser.add_argument(
        "--strategy-config",
        default=None,
        help="alias for --strategy (kept for ship-workflow compatibility)",
    )
    parser.add_argument(
        "--symbols",
        default=",".join(f"{k}={v}" for k, v in DEFAULT_BIND_SYMBOLS.items()),
        help="symbol bindings, e.g. A=BTCUSDT,B=ETHUSDT",
    )
    parser.add_argument("--timestamps", default=None, help="comma-separated epoch-ms timestamps for mock driver")
    parser.add_argument("--run-id", default=None, help="optional run_id override")
    return parser


async def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    strategy_name = str(args.strategy_config or args.strategy)
    bind_symbols = _parse_bind_symbols(str(args.symbols))
    timestamps = _parse_timestamps(args.timestamps)
    run_id = str(args.run_id or _make_run_id())
    init_logging(run_id=run_id)
    _set_current_run(run_id)
    engine, driver_cfg, _ingestion_plan = build_mock_engine(
        strategy_name=strategy_name,
        bind_symbols=bind_symbols,
        timestamps=timestamps,
    )
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
