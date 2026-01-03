from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ingestion.ohlcv.worker import OHLCVWorker
from ingestion.ohlcv.source import OHLCVWebSocketSource
from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer
from ingestion.orderbook.worker import OrderbookWorker
from ingestion.orderbook.source import OrderbookWebSocketSource
from ingestion.orderbook.normalize import BinanceOrderbookNormalizer
from quant_engine.runtime.modes import EngineMode
from quant_engine.runtime.realtime import RealtimeDriver
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.strategy.loader import StrategyLoader
from quant_engine.strategy.registry import get_strategy
from quant_engine.utils.logger import get_logger, init_logging, log_exception


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


# Optional domains (enable when you have realtime sources for them).
# NOTE: If your strategy requires these domains (REQUIRED_DATA), you MUST wire them below
# or fail-fast (we do fail-fast).


logger = get_logger(__name__)
DEFAULT_BIND_SYMBOLS = {"A": "BTCUSDT", "B": "ETHUSDT"}


def _build_realtime_ingestion_plan(
    engine: StrategyEngine,
    *,
    required_domains: set[str],
) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []

    def make_emit(primary_handler, *extra_handlers):
        def emit(tick):
            primary_handler.on_new_tick(tick)
            for h in extra_handlers:
                try:
                    h.on_new_tick(tick)
                except Exception:
                    log_exception(logger, "emit fan-out failed", handler=h, tick=tick)
        return emit

    # ---------- OHLCV (websocket) ----------
    for symbol, handler in engine.ohlcv_handlers.items():
        def _build_worker_ohlcv(symbol: str = symbol):
            # source = OHLCVWebSocketSource(symbol=symbol, interval=handler.interval)
            source = OHLCVWebSocketSource()
            normalizer = BinanceOHLCVNormalizer(symbol=symbol)
            return OHLCVWorker(source=source, normalizer=normalizer, symbol=symbol)

        plan.append(
            {
                "domain": "ohlcv",
                "symbol": symbol,
                "build_worker": _build_worker_ohlcv,
                "emit": make_emit(handler),
            }
        )

    # ---------- Orderbook (websocket) ----------
    for symbol, handler in engine.orderbook_handlers.items():
        def _build_worker_orderbook(symbol: str = symbol):
            # source = OrderbookWebSocketSource(symbol=symbol, depth=handler.depth)
            source = OrderbookWebSocketSource()
            normalizer = BinanceOrderbookNormalizer(symbol=symbol)
            return OrderbookWorker(source=source, normalizer=normalizer, symbol=symbol)

        plan.append(
            {
                "domain": "orderbook",
                "symbol": symbol,
                "build_worker": _build_worker_orderbook,
                "emit": make_emit(handler),
            }
        )

    # ---------- Option chain (poll/websocket) ----------
    if engine.option_chain_handlers:
        try:
            from ingestion.option_chain.worker import OptionChainWorker
            from ingestion.option_chain.source import OptionChainStreamSource
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
                def _build_worker_option_chain(asset: str = asset):
                    source = OptionChainStreamSource()
                    normalizer = DeribitOptionChainNormalizer(symbol=asset)
                    return OptionChainWorker(source=source, normalizer=normalizer, symbol=asset)

                # If IV surface handler exists for this asset, feed it the same chain ticks
                ivh = engine.iv_surface_handlers.get(asset)
                if ivh is not None:
                    emit = make_emit(ch, ivh)
                else:
                    emit = make_emit(ch)

                plan.append(
                    {
                        "domain": "option_chain",
                        "symbol": asset,
                        "build_worker": _build_worker_option_chain,
                        "emit": emit,
                    }
                )

    # ---------- Sentiment (poll) ----------
    if engine.sentiment_handlers:
        try:
            from ingestion.sentiment.worker import SentimentWorker
            from ingestion.sentiment.source import SentimentStreamSource
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
                def _build_worker_sentiment(src: str = src):
                    source = SentimentStreamSource()
                    normalizer = SentimentNormalizer(symbol=src, provider=src)
                    return SentimentWorker(source=source, normalizer=normalizer)

                plan.append(
                    {
                        "domain": "sentiment",
                        "symbol": src,
                        "build_worker": _build_worker_sentiment,
                        "emit": make_emit(sh),
                    }
                )

    return plan


def build_realtime_engine(
    *,
    strategy_name: str = "EXAMPLE",
    bind_symbols: dict[str, str] | None = None,
    overrides: dict | None = None,
) -> tuple[StrategyEngine, dict[str, Any], list[dict[str, Any]]]:
    StrategyCls = get_strategy(strategy_name)
    strategy = StrategyCls()
    if bind_symbols is None:
        bind_symbols = dict(DEFAULT_BIND_SYMBOLS)
    if bind_symbols:
        strategy = strategy.bind(**bind_symbols)

    engine = StrategyLoader.from_config(
        strategy=strategy,
        mode=EngineMode.REALTIME,
        overrides=overrides or {},
    )

    required_data = getattr(strategy, "REQUIRED_DATA", None)
    if isinstance(required_data, set):
        required_domains = {str(x) for x in required_data}
    elif isinstance(required_data, (list, tuple)):
        required_domains = {str(x) for x in required_data}
    else:
        required_domains = set()

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

    ingestion_plan = _build_realtime_ingestion_plan(engine, required_domains=required_domains)
    driver_cfg: dict[str, Any] = {}
    return engine, driver_cfg, ingestion_plan


async def main() -> None:
    run_id = _make_run_id()
    init_logging(run_id=run_id)
    _set_current_run(run_id)

    engine, _driver_cfg, ingestion_plan = build_realtime_engine()

    ingestion_tasks: list[asyncio.Task[None]] = []
    for entry in ingestion_plan:
        worker = entry["build_worker"]()
        ingestion_tasks.append(asyncio.create_task(worker.run(emit=entry["emit"])))

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
