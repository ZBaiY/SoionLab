from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ingestion.ohlcv.worker import OHLCVWorker
from ingestion.ohlcv.source import OHLCVFileSource
from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer
from ingestion.orderbook.worker import OrderbookWorker
from ingestion.orderbook.source import OrderbookFileSource
from ingestion.orderbook.normalize import BinanceOrderbookNormalizer
from ingestion.option_chain.worker import OptionChainWorker
from ingestion.option_chain.source import OptionChainFileSource
from ingestion.option_chain.normalize import DeribitOptionChainNormalizer
from ingestion.sentiment.worker import SentimentWorker
from ingestion.sentiment.source import SentimentFileSource
from ingestion.sentiment.normalize import SentimentNormalizer
from quant_engine.utils.logger import get_logger, init_logging, log_info, log_warn
from quant_engine.utils.guards import ensure_epoch_ms

from quant_engine.runtime.backtest import BacktestDriver
from quant_engine.runtime.modes import EngineMode
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.strategy.loader import StrategyLoader
from quant_engine.strategy.registry import get_strategy
from quant_engine.utils.paths import data_root_from_file


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
START_TS = 1622505600000  # 2021-06-01 00:00:00 UTC (epoch ms)
END_TS = 1622592000000    # 2021-06-02 00:00:00 UTC (epoch ms)
DATA_ROOT = data_root_from_file(__file__, levels_up=1)
DEFAULT_BIND_SYMBOLS = {"A": "BTCUSDT", "B": "ETHUSDT"}
OPTION_CHAIN_INTERVAL = "1m"


def _has_parquet_files(path: Path) -> bool:
    if not path.exists():
        return False
    if any(path.glob("*.parquet")):
        return True
    return any(path.rglob("*.parquet"))


def _has_jsonl_files(path: Path) -> bool:
    if not path.exists():
        return False
    if any(path.glob("*.jsonl")):
        return True
    return any(path.rglob("*.jsonl"))


def _has_ohlcv_data(root: Path, *, symbol: str, interval: str) -> bool:
    return _has_parquet_files(root / symbol / interval)


def _has_orderbook_data(root: Path, *, symbol: str) -> bool:
    path = root / symbol
    return path.exists() and any(path.glob("snapshot_*.parquet"))


def _has_option_chain_data(root: Path, *, asset: str, interval: str) -> bool:
    return _has_parquet_files(root / asset / interval)


def _has_sentiment_data(root: Path, *, provider: str) -> bool:
    return _has_jsonl_files(root / provider)


def _build_backtest_ingestion_plan(
    engine: StrategyEngine,
    *,
    data_root: Path,
    start_ts: int,
    end_ts: int,
    require_local_data: bool,
) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []

    # -------------------------
    # OHLCV ingestion
    # -------------------------
    for symbol, handler in engine.ohlcv_handlers.items():
        interval = handler.interval
        root = data_root / "raw" / "ohlcv"
        has_local_data = True
        if require_local_data:
            has_local_data = _has_ohlcv_data(root, symbol=symbol, interval=interval)

        def _build_worker_ohlcv(
            *,
            symbol: str = symbol,
            interval: str = interval,
            root: Path = root,
            start_ts: int = start_ts,
            end_ts: int = end_ts,
        ):
            source = OHLCVFileSource(
                root=root,
                symbol=symbol,
                interval=interval,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            normalizer = BinanceOHLCVNormalizer(symbol=symbol)
            return OHLCVWorker(
                source=source,
                normalizer=normalizer,
                symbol=symbol,
                poll_interval=None,  # backtest: do not throttle
            )

        plan.append(
            {
                "domain": "ohlcv",
                "symbol": symbol,
                "source_type": "OHLCVFileSource",
                "root": str(root),
                "interval": interval,
                "has_local_data": has_local_data,
                "build_worker": _build_worker_ohlcv,
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )

    # -------------------------
    # Orderbook ingestion
    # -------------------------
    for symbol, _handler in engine.orderbook_handlers.items():
        root = data_root / "raw" / "orderbook"
        has_local_data = True
        if require_local_data:
            has_local_data = _has_orderbook_data(root, symbol=symbol)

        def _build_worker_orderbook(
            *,
            symbol: str = symbol,
            root: Path = root,
            start_ts: int = start_ts,
            end_ts: int = end_ts,
        ):
            source = OrderbookFileSource(
                root=root,
                symbol=symbol,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            normalizer = BinanceOrderbookNormalizer(symbol=symbol)
            return OrderbookWorker(
                source=source,
                normalizer=normalizer,
                symbol=symbol,
                poll_interval=None,  # backtest: do not throttle
            )

        plan.append(
            {
                "domain": "orderbook",
                "symbol": symbol,
                "source_type": "OrderbookFileSource",
                "root": str(root),
                "has_local_data": has_local_data,
                "build_worker": _build_worker_orderbook,
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )

    # -------------------------
    # Option chain ingestion
    # -------------------------
    for asset, _handler in engine.option_chain_handlers.items():
        root = data_root / "raw" / "option_chain"
        has_local_data = True
        if require_local_data:
            has_local_data = _has_option_chain_data(root, asset=asset, interval=OPTION_CHAIN_INTERVAL)

        def _build_worker_option_chain(
            *,
            asset: str = asset,
            root: Path = root,
            start_ts: int = start_ts,
            end_ts: int = end_ts,
        ):
            source = OptionChainFileSource(
                root=root,
                asset=asset,
                interval=OPTION_CHAIN_INTERVAL,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            normalizer = DeribitOptionChainNormalizer(symbol=asset)
            return OptionChainWorker(
                source=source,
                normalizer=normalizer,
                symbol=asset,
                poll_interval=None,  # backtest: do not throttle
            )

        plan.append(
            {
                "domain": "option_chain",
                "symbol": asset,
                "source_type": "OptionChainFileSource",
                "root": str(root),
                "interval": OPTION_CHAIN_INTERVAL,
                "has_local_data": has_local_data,
                "build_worker": _build_worker_option_chain,
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )

    # -------------------------
    # Sentiment ingestion
    # -------------------------
    for src, _handler in engine.sentiment_handlers.items():
        root = data_root / "raw" / "sentiment"
        has_local_data = True
        if require_local_data:
            has_local_data = _has_sentiment_data(root, provider=src)

        def _build_worker_sentiment(
            *,
            src: str = src,
            root: Path = root,
            start_ts: int = start_ts,
            end_ts: int = end_ts,
        ):
            source = SentimentFileSource(
                root=root,
                provider=src,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            normalizer = SentimentNormalizer(symbol=src, provider=src)
            return SentimentWorker(
                source=source,
                normalizer=normalizer,
                poll_interval=None,  # backtest: do not throttle
            )

        plan.append(
            {
                "domain": "sentiment",
                "symbol": src,
                "source_type": "SentimentFileSource",
                "root": str(root),
                "has_local_data": has_local_data,
                "build_worker": _build_worker_sentiment,
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )

    return plan


def build_backtest_engine(
    *,
    strategy_name: str = "EXAMPLE",
    bind_symbols: dict[str, str] | None = None,
    overrides: dict | None = None,
    start_ts: int = START_TS,
    end_ts: int = END_TS,
    data_root: Path = DATA_ROOT,
    require_local_data: bool = True,
) -> tuple[StrategyEngine, dict[str, int], list[dict[str, Any]]]:
    StrategyCls = get_strategy(strategy_name)
    strategy = StrategyCls()
    if bind_symbols is None:
        bind_symbols = dict(DEFAULT_BIND_SYMBOLS)
    if bind_symbols:
        strategy = strategy.bind(**bind_symbols)

    engine = StrategyLoader.from_config(
        strategy=strategy,
        mode=EngineMode.BACKTEST,
        overrides=overrides or {},
    )
    driver_cfg = {"start_ts": int(start_ts), "end_ts": int(end_ts)}
    ingestion_plan = _build_backtest_ingestion_plan(
        engine,
        data_root=data_root,
        start_ts=int(start_ts),
        end_ts=int(end_ts),
        require_local_data=require_local_data,
    )
    return engine, driver_cfg, ingestion_plan


async def main() -> None:
    run_id = _make_run_id()
    init_logging(run_id=run_id)
    _set_current_run(run_id)
    # -------------------------------------------------
    # 1. Load & bind strategy
    # -------------------------------------------------
    engine, driver_cfg, ingestion_plan = build_backtest_engine()
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
        ts = ensure_epoch_ms(getattr(tick, "timestamp"))
        await tick_queue.put((ts, _seq, tick))
        _seq += 1

    for entry in ingestion_plan:
        if not entry["has_local_data"]:
            log_warn(
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
