from __future__ import annotations

import asyncio

from quant_engine.runtime.modes import EngineMode
from quant_engine.runtime.realtime import RealtimeDriver
from quant_engine.strategy.registry import get_strategy
from quant_engine.utils.logger import get_logger

# --- ingestion (external to runtime) ---
from ingestion.ohlcv.worker import OHLCVWorker
from ingestion.ohlcv.source import OHLCVWebSocketSource
from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer

from ingestion.orderbook.worker import OrderbookWorker
from ingestion.orderbook.source import OrderbookWebSocketSource
from ingestion.orderbook.normalize import BinanceOrderbookNormalizer

# Optional domains (enable when you have realtime sources for them).
# NOTE: If your strategy requires these domains (REQUIRED_DATA), you MUST wire them below
# or fail-fast (we do fail-fast).


logger = get_logger(__name__)


async def main() -> None:
    # -------------------------------------------------
    # 1) Load & bind strategy (NO time semantics)
    # -------------------------------------------------
    StrategyCls = get_strategy("EXAMPLE")
    strategy = StrategyCls().bind(A="BTCUSDT", B="ETHUSDT")

    # -------------------------------------------------
    # 2) Build engine (runtime semantics only)
    # -------------------------------------------------
    engine = strategy.build(mode=EngineMode.REALTIME)

    required_data = getattr(strategy, "REQUIRED_DATA", None)
    if isinstance(required_data, set):
        required_domains = {str(x) for x in required_data}
    elif isinstance(required_data, (list, tuple)):
        required_domains = {str(x) for x in required_data}
    else:
        required_domains = set()

    # -------------------------------------------------
    # 3) Start ingestion workers (async, external world)
    #    They push ticks into handler caches via handler.on_new_tick(...)
    # -------------------------------------------------
    ingestion_tasks: list[asyncio.Task[None]] = []

    def make_emit(primary_handler, *extra_handlers):
        def emit(tick):
            primary_handler.on_new_tick(tick)
            for h in extra_handlers:
                try:
                    h.on_new_tick(tick)
                except Exception:
                    logger.exception("emit fan-out failed", extra={"handler": h, "tick": tick})
        return emit

    # ---------- OHLCV (websocket) ----------
    for symbol, handler in engine.ohlcv_handlers.items():
        # source = OHLCVWebSocketSource(symbol=symbol, interval=handler.interval)
        source = OHLCVWebSocketSource()
        normalizer = BinanceOHLCVNormalizer(symbol=symbol)
        worker = OHLCVWorker(source=source, normalizer=normalizer, symbol=symbol)
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=make_emit(handler))))

    # ---------- Orderbook (websocket) ----------
    for symbol, handler in engine.orderbook_handlers.items():
        # source = OrderbookWebSocketSource(symbol=symbol, depth=handler.depth)
        source = OrderbookWebSocketSource()
        normalizer = BinanceOrderbookNormalizer(symbol=symbol)
        worker = OrderbookWorker(source=source, normalizer=normalizer, symbol=symbol)
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=make_emit(handler))))

    # ---------- Option chain (poll/websocket) ----------
    if engine.option_chain_handlers:
        try:
            from ingestion.option_chain.worker import OptionChainWorker
            from ingestion.option_chain.source import OptionChainStreamSource, DeribitOptionChainRESTSource
            from ingestion.option_chain.normalize import DeribitOptionChainNormalizer
        except Exception as e:
            if "option_chain" in required_domains:
                raise RuntimeError(
                    "Strategy requires option_chain, but realtime ingestion wiring is missing. "
                    "Implement/enable ingestion.option_chain.{source,worker,normalize} realtime sources."
                ) from e
            OptionChainWorker = None  # type: ignore
        if OptionChainWorker is not None:
            for asset, ch in engine.option_chain_handlers.items():
                # Prefer polling unless you explicitly implement websocket
                # source = DeribitOptionChainRESTSource(currency=asset, poll_interval=60.0)
                source = OptionChainStreamSource()
                normalizer = DeribitOptionChainNormalizer(symbol=asset)
                worker = OptionChainWorker(source=source, normalizer=normalizer, symbol=asset)

                # If IV surface handler exists for this asset, feed it the same chain ticks
                ivh = engine.iv_surface_handlers.get(asset)
                if ivh is not None:
                    emit = make_emit(ch, ivh)
                else:
                    emit = make_emit(ch)

                ingestion_tasks.append(asyncio.create_task(worker.run(emit=emit)))

    # ---------- Sentiment (poll) ----------
    if engine.sentiment_handlers:
        try:
            from ingestion.sentiment.worker import SentimentWorker
            from ingestion.sentiment.source import SentimentRESTSource, SentimentStreamSource
            from ingestion.sentiment.normalize import SentimentNormalizer
        except Exception as e:
            if "sentiment" in required_domains:
                raise RuntimeError(
                    "Strategy requires sentiment, but realtime ingestion wiring is missing. "
                    "Implement/enable ingestion.sentiment.{source,worker,normalize} realtime sources."
                ) from e
            SentimentWorker = None  # type: ignore
        if SentimentWorker is not None:
            for src, sh in engine.sentiment_handlers.items():
                # source = SentimentRESTSource(source=src, interval=sh.interval)
                source = SentimentStreamSource()
                normalizer = SentimentNormalizer(symbol=src, provider=src)
                worker = SentimentWorker(source=source, normalizer=normalizer)
                ingestion_tasks.append(asyncio.create_task(worker.run(emit=make_emit(sh))))

    # Fail-fast if the strategy declares REQUIRED_DATA but app didn’t wire the domain
    if required_domains:
        missing: list[str] = []
        if "ohlcv" in required_domains and not engine.ohlcv_handlers:
            missing.append("ohlcv")
        if "orderbook" in required_domains and not engine.orderbook_handlers:
            missing.append("orderbook")
        if "option_chain" in required_domains and not engine.option_chain_handlers:
            missing.append("option_chain")
        if "sentiment" in required_domains and not engine.sentiment_handlers:
            missing.append("sentiment")
        if "iv_surface" in required_domains and not engine.iv_surface_handlers:
            missing.append("iv_surface")
        if missing:
            raise RuntimeError(f"Strategy requires domains not present in engine: {missing}")

    logger.info("Realtime ingestion workers started.")

    # -------------------------------------------------
    # 4) Run realtime driver (single time authority)
    # -------------------------------------------------
    driver = RealtimeDriver(engine=engine, spec=engine.spec)

    try:
        logger.info("Starting realtime driver...")
        await driver.run()
    finally:
        logger.info("Shutting down ingestion workers...")
        for t in ingestion_tasks:
            t.cancel()
        if ingestion_tasks:
            await asyncio.gather(*ingestion_tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
