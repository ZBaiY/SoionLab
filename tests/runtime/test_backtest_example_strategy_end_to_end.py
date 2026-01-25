from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import re

import pytest

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
from ingestion.contracts.tick import _to_interval_ms

from quant_engine.runtime.backtest import BacktestDriver
from quant_engine.runtime.modes import EngineMode
from quant_engine.strategy.registry import get_strategy
from quant_engine.utils.cleaned_path_resolver import base_asset_from_symbol
from quant_engine.utils.guards import ensure_epoch_ms
from quant_engine.utils.logger import get_logger, log_info, log_warn
from quant_engine.utils.paths import data_root_from_file


def _find_parquet_files(path: Path) -> list[Path]:
    if not path.exists():
        return []
    files = list(path.glob("*.parquet"))
    if files:
        return files
    return list(path.rglob("*.parquet"))


def _earliest_ohlcv_ts_ms(path: Path) -> int | None:
    files = _find_parquet_files(path)
    if not files:
        return None
    dates: list[datetime] = []
    for fp in files:
        stem = fp.stem
        if re.fullmatch(r"\\d{4}", stem):
            dates.append(datetime(int(stem), 1, 1, tzinfo=timezone.utc))
            continue
        m = re.search(r"(\\d{4})_(\\d{2})_(\\d{2})", stem)
        if m:
            y, mo, d = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
            dates.append(datetime(y, mo, d, tzinfo=timezone.utc))
    if not dates:
        return None
    return int(min(dates).timestamp() * 1000)


def _has_orderbook_data(path: Path) -> bool:
    return path.exists() and any(path.glob("snapshot_*.parquet"))


def _has_sentiment_data(path: Path) -> bool:
    if not path.exists():
        return False
    return any(path.glob("*.jsonl")) or any(path.rglob("*.jsonl"))

START_TS = 1766966400000  # 2025-12-29 00:00:00 UTC (epoch ms)
END_TS = 1767052800000    # 2025-12-30 00:00:00 UTC (epoch ms)

@pytest.mark.integration
@pytest.mark.local
@pytest.mark.asyncio
async def test_backtest_example_strategy_end_to_end() -> None:
    logger = get_logger(__name__)
    data_root = data_root_from_file(__file__, levels_up=2)
    StrategyCls = get_strategy("EXAMPLE")
    strategy = StrategyCls().bind(A="BTCUSDT", B="ETHUSDT")
    engine = strategy.build(mode=EngineMode.BACKTEST)

    ohlcv_root = data_root / "cleaned" / "ohlcv"
    ohlcv_symbols = list(engine.ohlcv_handlers.keys())
    ohlcv_intervals = {
        sym: getattr(engine.ohlcv_handlers[sym], "interval", None) for sym in ohlcv_symbols
    }
    missing_ohlcv = [
        sym
        for sym in ohlcv_symbols
        if not ohlcv_intervals.get(sym) or not _find_parquet_files(ohlcv_root / sym / str(ohlcv_intervals[sym]))
    ]
    if missing_ohlcv:
        pytest.skip(f"Missing local OHLCV data for symbols: {missing_ohlcv}")
    # start_ts_candidates = [
    #     _earliest_ohlcv_ts_ms(ohlcv_root / sym / str(ohlcv_intervals[sym]))
    #     for sym in ohlcv_symbols
    # ]
    # start_ts_candidates = [ts for ts in start_ts_candidates if ts is not None]
    
    # if not start_ts_candidates:
    #     pytest.skip("Unable to determine OHLCV start_ts from local data")

    start_ts = START_TS
    end_ts = start_ts + 2 * int(engine.spec.interval_ms)

    tick_queue: asyncio.PriorityQueue[tuple[int, int, object]] = asyncio.PriorityQueue(maxsize=1024)
    seq = 0

    async def emit_to_queue(tick: object) -> None:
        nonlocal seq
        ts = ensure_epoch_ms(getattr(tick, "data_ts", None))
        seq_key = seq
        seq += 1
        await tick_queue.put((ts, -seq_key, tick))

    ingestion_tasks: list[asyncio.Task[None]] = []
    skipped_domains: set[str] = set()

    for symbol, handler in engine.ohlcv_handlers.items():
        interval = getattr(handler, "interval", None)
        interval_ms = _to_interval_ms(interval) if isinstance(interval, str) and interval else None
        has_local_data = bool(interval) and _find_parquet_files(ohlcv_root / symbol / str(interval))
        if not has_local_data:
            log_warn(
                logger,
                "ingestion.worker.skipped_no_data",
                domain="ohlcv",
                symbol=symbol,
                root=str(ohlcv_root),
            )
            skipped_domains.add("ohlcv")
            continue
        source = OHLCVFileSource(
            root=ohlcv_root,
            symbol=symbol,
            interval=str(interval),
            start_ts=start_ts,
            end_ts=end_ts,
        )
        normalizer = BinanceOHLCVNormalizer(symbol=symbol)
        worker = OHLCVWorker(
            source=source,
            normalizer=normalizer,
            symbol=symbol,
            interval=str(interval) if interval else None,
            interval_ms=int(interval_ms) if interval_ms is not None else None,
            poll_interval=None,
        )
        log_info(
            logger,
            "ingestion.worker.start",
            domain="ohlcv",
            symbol=symbol,
            source_type=type(source).__name__,
            has_local_data=has_local_data,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit_to_queue)))

    orderbook_root = data_root / "cleaned" / "orderbook"
    for symbol, handler in engine.orderbook_handlers.items():
        interval = getattr(handler, "interval", None)
        interval_ms = _to_interval_ms(interval) if isinstance(interval, str) and interval else None
        has_local_data = _has_orderbook_data(orderbook_root / symbol)
        if not has_local_data:
            log_warn(
                logger,
                "ingestion.worker.skipped_no_data",
                domain="orderbook",
                symbol=symbol,
                root=str(orderbook_root),
            )
            skipped_domains.add("orderbook")
            continue
        source = OrderbookFileSource(
            root=orderbook_root,
            symbol=symbol,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        normalizer = BinanceOrderbookNormalizer(symbol=symbol)
        worker = OrderbookWorker(
            source=source,
            normalizer=normalizer,
            symbol=symbol,
            interval=str(interval) if interval else None,
            interval_ms=int(interval_ms) if interval_ms is not None else None,
            poll_interval=None,
        )
        log_info(
            logger,
            "ingestion.worker.start",
            domain="orderbook",
            symbol=symbol,
            source_type=type(source).__name__,
            has_local_data=has_local_data,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit_to_queue)))

    option_chain_root = data_root / "cleaned" / "option_chain"
    for symbol, handler in engine.option_chain_handlers.items():
        interval = getattr(handler, "interval", None)
        interval_ms = _to_interval_ms(interval) if isinstance(interval, str) and interval else None
        asset = base_asset_from_symbol(symbol)
        has_local_data = _find_parquet_files(option_chain_root / asset / "1m")
        if not has_local_data:
            log_warn(
                logger,
                "ingestion.worker.skipped_no_data",
                domain="option_chain",
                symbol=symbol,
                root=str(option_chain_root),
            )
            skipped_domains.add("option_chain")
            continue
        source = OptionChainFileSource(
            root=option_chain_root,
            asset=asset,
            interval="1m",
            start_ts=start_ts,
            end_ts=end_ts,
        )
        normalizer = DeribitOptionChainNormalizer(symbol=symbol)
        worker = OptionChainWorker(
            source=source,
            normalizer=normalizer,
            symbol=symbol,
            interval=str(interval) if interval else None,
            interval_ms=int(interval_ms) if interval_ms is not None else None,
            poll_interval=None,
        )
        log_info(
            logger,
            "ingestion.worker.start",
            domain="option_chain",
            symbol=symbol,
            source_type=type(source).__name__,
            has_local_data=bool(has_local_data),
            start_ts=start_ts,
            end_ts=end_ts,
        )
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit_to_queue)))

    sentiment_root = data_root / "cleaned" / "sentiment"
    for src, handler in engine.sentiment_handlers.items():
        interval = getattr(handler, "interval", None)
        interval_ms = _to_interval_ms(interval) if isinstance(interval, str) and interval else None
        has_local_data = _has_sentiment_data(sentiment_root / src)
        if not has_local_data:
            log_warn(
                logger,
                "ingestion.worker.skipped_no_data",
                domain="sentiment",
                symbol=src,
                root=str(sentiment_root),
            )
            skipped_domains.add("sentiment")
            continue
        source = SentimentFileSource(
            root=sentiment_root,
            provider=src,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        normalizer = SentimentNormalizer(symbol=src, provider=src)
        worker = SentimentWorker(
            source=source,
            normalizer=normalizer,
            interval=str(interval) if interval else None,
            interval_ms=int(interval_ms) if interval_ms is not None else None,
            poll_interval=None,
        )
        log_info(
            logger,
            "ingestion.worker.start",
            domain="sentiment",
            symbol=src,
            source_type=type(source).__name__,
            has_local_data=has_local_data,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit_to_queue)))

    driver = BacktestDriver(
        engine=engine,
        spec=engine.spec,
        start_ts=start_ts,
        end_ts=end_ts,
        tick_queue=tick_queue,
        ingestion_tasks=ingestion_tasks,
    )
    try:
        await driver.run()
    finally:
        for t in ingestion_tasks:
            t.cancel()
        if ingestion_tasks:
            await asyncio.gather(*ingestion_tasks, return_exceptions=True)

    if ingestion_tasks:
        assert driver.snapshots

    timestamps = [snap.timestamp for snap in driver.snapshots]
    assert timestamps == sorted(timestamps)
    if skipped_domains:
        domain_maps = {
            "orderbook": engine.orderbook_handlers,
            "option_chain": engine.option_chain_handlers,
            "sentiment": engine.sentiment_handlers,
        }
        for domain in skipped_domains:
            assert domain_maps.get(domain)
