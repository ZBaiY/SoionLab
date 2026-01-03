from __future__ import annotations

from quant_engine.runtime.modes import EngineMode
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.strategy.loader import StrategyLoader
from quant_engine.strategy.registry import get_strategy


def _build_engine(strategy_name: str, *, bind_symbols: dict[str, str]) -> StrategyEngine:
    StrategyCls = get_strategy(strategy_name)
    strategy = StrategyCls().bind(**bind_symbols)
    strategy.standardize({})
    return StrategyLoader.from_config(strategy=strategy, mode=EngineMode.BACKTEST, overrides={})


def test_loader_builds_example_with_unused_handlers() -> None:
    engine = _build_engine("EXAMPLE", bind_symbols={"A": "BTCUSDT", "B": "ETHUSDT"})
    assert engine.ohlcv_handlers
    assert engine.orderbook_handlers
    assert engine.option_chain_handlers
    assert engine.iv_surface_handlers
    assert engine.sentiment_handlers
    assert engine.feature_extractor is not None
    assert engine.models
    assert engine.decision is not None
    assert engine.risk_manager is not None
    assert engine.execution_engine is not None
    assert engine.portfolio is not None


def test_loader_builds_rsi_adx_without_model() -> None:
    engine = _build_engine("RSI_ADX_SIDEWAYS", bind_symbols={"A": "BTCUSDT"})
    assert engine.ohlcv_handlers
    assert engine.models == {}
    assert engine.decision is not None
    assert engine.risk_manager is not None
    assert engine.execution_engine is not None
    assert engine.portfolio is not None
