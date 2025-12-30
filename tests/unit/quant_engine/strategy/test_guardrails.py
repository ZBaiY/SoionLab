import pytest

from quant_engine.exceptions.core import FatalError
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.strategy.engine import StrategyEngine
from ingestion.contracts.tick import IngestionTick


class _DummyPortfolio:
    def state(self):
        return {}


class _DummyFeatureExtractor:
    required_windows = {"ohlcv": 1}
    warmup_steps = 0

    def update(self, timestamp: int):
        return {}


class _DummyDecision:
    def decide(self, context):
        return 0.0


class _DummyRisk:
    def adjust(self, score, context):
        return 0.0


class _DummyExecution:
    def execute(self, target_position, portfolio_state, market_data, timestamp):
        return []


def test_monotonic_guard_on_ticks():
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT")
    engine = StrategyEngine(
        spec=spec,
        ohlcv_handlers={},
        orderbook_handlers={},
        option_chain_handlers={},
        iv_surface_handlers={},
        sentiment_handlers={},
        trades_handlers={},
        option_trades_handlers={},
        feature_extractor=_DummyFeatureExtractor(),
        models={},
        decision=_DummyDecision(),
        risk_manager=_DummyRisk(),
        execution_engine=_DummyExecution(),
        portfolio_manager=_DummyPortfolio(),
        guardrails=True,
    )

    t1 = IngestionTick(timestamp=1, data_ts=2000, domain="trades", symbol="BTCUSDT", payload={})
    t2 = IngestionTick(timestamp=2, data_ts=1000, domain="trades", symbol="BTCUSDT", payload={})

    engine.ingest_tick(t1)
    with pytest.raises(FatalError):
        engine.ingest_tick(t2)
