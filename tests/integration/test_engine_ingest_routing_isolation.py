from __future__ import annotations

from types import SimpleNamespace

from ingestion.contracts.tick import IngestionTick
from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.data.sentiment.sentiment_handler import SentimentDataHandler
from quant_engine.execution.engine import ExecutionEngine
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.strategy.engine import StrategyEngine
from tests.helpers.fakes_runtime import (
    DummyDecision,
    DummyMatcher,
    DummyModel,
    DummyPolicy,
    DummyPortfolio,
    DummyRisk,
    DummyRouter,
    DummySlippage,
)


def test_engine_ingest_routing_isolation() -> None:
    def update(timestamp: int | None = None) -> dict[str, float]:
        return {}

    feature_extractor = SimpleNamespace(
        required_windows={"ohlcv": 1},
        warmup_steps=1,
        update=update,
    )

    spec = EngineSpec.from_interval(
        mode=EngineMode.BACKTEST,
        interval="1s",
        symbol="BTCUSDT",
    )
    handler_btc = OHLCVDataHandler("BTCUSDT", interval="1m", mode=EngineMode.MOCK)
    handler_eth = OHLCVDataHandler("ETHUSDT", interval="1m", mode=EngineMode.MOCK)
    sentiment_handler = SentimentDataHandler("BTCUSDT", mode=EngineMode.MOCK)

    engine = StrategyEngine(
        spec=spec,
        ohlcv_handlers={"BTCUSDT": handler_btc, "ETHUSDT": handler_eth},
        orderbook_handlers={},
        option_chain_handlers={},
        iv_surface_handlers={},
        sentiment_handlers={"BTCUSDT": sentiment_handler},
        trades_handlers={},
        option_trades_handlers={},
        feature_extractor=feature_extractor,
        models={"main": DummyModel("BTCUSDT")},
        decision=DummyDecision(),
        risk_manager=DummyRisk("BTCUSDT"),
        execution_engine=ExecutionEngine(
            DummyPolicy("BTCUSDT"),
            DummyRouter("BTCUSDT"),
            DummySlippage("BTCUSDT"),
            DummyMatcher("BTCUSDT"),
        ),
        portfolio_manager=DummyPortfolio("BTCUSDT"),
        guardrails=True,
    )

    btc_seen: list[tuple[str, str, str | None]] = []
    eth_seen: list[tuple[str, str, str | None]] = []
    sentiment_seen: list[tuple[str, str, str | None]] = []

    handler_btc.on_new_tick = lambda tick: btc_seen.append((
        tick.domain,
        tick.symbol,
        tick.payload.get("sentinel"),
    ))
    handler_eth.on_new_tick = lambda tick: eth_seen.append((
        tick.domain,
        tick.symbol,
        tick.payload.get("sentinel"),
    ))
    sentiment_handler.on_new_tick = lambda tick: sentiment_seen.append((
        tick.domain,
        tick.symbol,
        tick.payload.get("sentinel"),
    ))

    base_ts = 1_700_000_000_000
    btc_source = handler_btc.source_id
    eth_source = handler_eth.source_id
    sent_source = sentiment_handler.source_id
    ticks = [
        IngestionTick(
            timestamp=base_ts,
            data_ts=base_ts,
            domain="ohlcv",
            symbol="BTCUSDT",
            payload={"sentinel": "btc-1"},
            source_id=btc_source,
        ),
        IngestionTick(
            timestamp=base_ts + 1000,
            data_ts=base_ts + 1000,
            domain="ohlcv",
            symbol="ETHUSDT",
            payload={"sentinel": "eth-1"},
            source_id=eth_source,
        ),
        IngestionTick(
            timestamp=base_ts + 2000,
            data_ts=base_ts + 2000,
            domain="sentiment",
            symbol="BTCUSDT",
            payload={"sentinel": "sent-1", "score": 0.1},
            source_id=sent_source,
        ),
        IngestionTick(
            timestamp=base_ts + 3000,
            data_ts=base_ts + 3000,
            domain="ohlcv",
            symbol="XRPUSDT",
            payload={"sentinel": "xrp-1"},
            source_id=btc_source,
        ),
        IngestionTick(
            timestamp=base_ts + 4000,
            data_ts=base_ts + 4000,
            domain="ohlcv",
            symbol="BTCUSDT",
            payload={"sentinel": "btc-2"},
            source_id=btc_source,
        ),
        IngestionTick(
            timestamp=base_ts + 5000,
            data_ts=base_ts + 5000,
            domain="sentiment",
            symbol="ETHUSDT",
            payload={"sentinel": "sent-eth"},
            source_id=sent_source,
        ),
    ]

    for tick in ticks:
        engine.ingest_tick(tick)

    assert btc_seen == [("ohlcv", "BTCUSDT", "btc-1"), ("ohlcv", "BTCUSDT", "btc-2")]
    assert eth_seen == [("ohlcv", "ETHUSDT", "eth-1")]
    assert sentiment_seen == [("sentiment", "BTCUSDT", "sent-1")]
