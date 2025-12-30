from __future__ import annotations

import asyncio
from pathlib import Path

from quant_engine.utils.logger import get_logger, init_logging

init_logging()

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

from quant_engine.runtime.backtest import BacktestDriver
from quant_engine.runtime.modes import EngineMode
from quant_engine.strategy.registry import get_strategy
from quant_engine.utils.paths import data_root_from_file


logger = get_logger(__name__)
START_TS = 1622505600  # 2021-06-01 00:00:00 UTC
END_TS = 1622592000    # 2021-06-02 00:00:00 UTC
DATA_ROOT = data_root_from_file(__file__, levels_up=1)


async def main() -> None:
    # -------------------------------------------------
    # 1. Load & bind strategy
    # -------------------------------------------------
    StrategyCls = get_strategy("EXAMPLE")

    strategy = StrategyCls().bind(A="BTCUSDT", B="ETHUSDT")

    engine = strategy.build(mode=EngineMode.BACKTEST)

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
        # Use integer microseconds to avoid float boundary issues
        ts = getattr(tick, "timestamp")
        ts_us = int(round(float(ts) * 1_000_000))
        await tick_queue.put((ts_us, _seq, tick))
        _seq += 1

    # -------------------------
    # OHLCV ingestion
    # -------------------------
    for symbol, handler in engine.ohlcv_handlers.items():
        source = OHLCVFileSource(
            root=DATA_ROOT / "raw" / "klines",
            symbol=symbol,
            interval=handler.interval,
        )
        normalizer = BinanceOHLCVNormalizer(symbol=symbol)
        worker = OHLCVWorker(
            source=source,
            normalizer=normalizer,
            symbol=symbol,
            poll_interval=None,  # backtest: do not throttle
        )
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit_to_queue)))

    # -------------------------
    # Orderbook ingestion
    # -------------------------
    for symbol, handler in engine.orderbook_handlers.items():
        source = OrderbookFileSource(
            root=DATA_ROOT / "raw" / "orderbook",
            symbol=symbol,
        )
        normalizer = BinanceOrderbookNormalizer(symbol=symbol)
        worker = OrderbookWorker(
            source=source,
            normalizer=normalizer,
            symbol=symbol,
            poll_interval=None,  # backtest: do not throttle
        )
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit_to_queue)))

    # -------------------------
    # Option chain ingestion
    # -------------------------
    for asset, handler in engine.option_chain_handlers.items():
        source = OptionChainFileSource(
            root=DATA_ROOT / "raw" / "option_chain",
            asset=asset,
            interval="1m",
        )
        normalizer = DeribitOptionChainNormalizer(symbol=asset)
        worker = OptionChainWorker(
            source=source,
            normalizer=normalizer,
            symbol=asset,
            poll_interval=None,  # backtest: do not throttle
        )
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit_to_queue)))

    # -------------------------
    # Sentiment ingestion
    # -------------------------
    for src, handler in engine.sentiment_handlers.items():
        source = SentimentFileSource(
            root=DATA_ROOT / "raw" / "sentiment",
            provider=src,
        )
        normalizer = SentimentNormalizer(symbol=src, provider=src)
        worker = SentimentWorker(
            source=source,
            normalizer=normalizer,
            # provider=src,
            poll_interval=None,  # backtest: do not throttle
        )
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit_to_queue)))

    # -------------------------------------------------
    # 3. Run deterministic backtest (time only)
    # -------------------------------------------------
    driver = BacktestDriver(
        engine=engine,
        spec=engine.spec,
        start_ts=START_TS,
        end_ts=END_TS,
        tick_queue=tick_queue,
    )

    logger.info("Starting backtest driver...")
    await driver.run()

    logger.info("Stopping ingestion workers...")
    for t in ingestion_tasks:
        t.cancel()
    if ingestion_tasks:
        await asyncio.gather(*ingestion_tasks, return_exceptions=True)

    # -------------------------------------------------
    # 4. Final snapshot / reports
    # -------------------------------------------------
    logger.info("Backtest finished.")
    logger.info("Final portfolio: %s", engine.portfolio.state().to_dict())


if __name__ == "__main__":
    asyncio.run(main())
