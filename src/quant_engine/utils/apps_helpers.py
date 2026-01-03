from quant_engine.runtime.modes import EngineMode
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.strategy.loader import StrategyLoader
from quant_engine.strategy.registry import get_strategy

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
from pathlib import Path
from typing import Any
from quant_engine.utils.paths import data_root_from_file


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
    option_chain_interval: str,
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
            has_local_data = _has_option_chain_data(root, asset=asset, interval=option_chain_interval)

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
                interval=option_chain_interval,
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
                "interval": option_chain_interval,
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
    start_ts: int = 1622505600000,
    end_ts: int = 1622592000000,
    data_root: Path | None = None,
    require_local_data: bool = True,
    option_chain_interval: str = "1m",
) -> tuple[StrategyEngine, dict[str, int], list[dict[str, Any]]]:
    StrategyCls = get_strategy(strategy_name)

    if bind_symbols is None:
        bind_symbols = {"A": "BTCUSDT", "B": "ETHUSDT"}
    if data_root is None:
        data_root = data_root_from_file(__file__, levels_up=1)
    cfg = StrategyCls.standardize(overrides=overrides or {}, symbols=bind_symbols)

    engine = StrategyLoader.from_config(
        strategy=cfg,
        mode=EngineMode.BACKTEST,
        overrides={},
    )
    driver_cfg = {"start_ts": int(start_ts), "end_ts": int(end_ts)}
    ingestion_plan = _build_backtest_ingestion_plan(
        engine,
        data_root=data_root,
        start_ts=int(start_ts),
        end_ts=int(end_ts),
        require_local_data=require_local_data,
        option_chain_interval=option_chain_interval,
    )
    return engine, driver_cfg, ingestion_plan
