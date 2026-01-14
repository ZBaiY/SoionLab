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
    # assert engine.orderbook_handlers -- deactivated
    assert engine.option_chain_handlers
    # assert engine.iv_surface_handlers
    # assert engine.sentiment_handlers
    assert engine.feature_extractor is not None
    assert engine.models
    assert engine.decision is not None
    assert engine.risk_manager is not None
    assert engine.execution_engine is not None
    assert engine.portfolio is not None


def test_loader_builds_rsi_adx_without_model() -> None:
    engine = _build_engine("RSI-ADX-SIDEWAYS", bind_symbols={"A": "BTCUSDT", 'window_RSI' : '14', 'window_ADX': '14', 'window_RSI_rolling': '5'})
    assert engine.ohlcv_handlers
    assert engine.models == {}
    assert engine.decision is not None
    assert engine.risk_manager is not None
    assert engine.execution_engine is not None
    assert engine.portfolio is not None


def test_loader_inits_portfolio_before_risk(monkeypatch) -> None:
    order = []
    from quant_engine.portfolio.loader import PortfolioLoader
    from quant_engine.risk.loader import RiskLoader
    from quant_engine.execution.loader import ExecutionLoader

    orig_portfolio = PortfolioLoader.from_config
    orig_risk = RiskLoader.from_config
    orig_exec = ExecutionLoader.from_config

    def _portfolio(*args, **kwargs):
        order.append("portfolio")
        return orig_portfolio(*args, **kwargs)

    def _risk(*args, **kwargs):
        order.append("risk")
        return orig_risk(*args, **kwargs)

    def _exec(*args, **kwargs):
        order.append("execution")
        return orig_exec(*args, **kwargs)

    monkeypatch.setattr(PortfolioLoader, "from_config", _portfolio)
    monkeypatch.setattr(RiskLoader, "from_config", _risk)
    monkeypatch.setattr(ExecutionLoader, "from_config", _exec)

    _ = _build_engine("EXAMPLE", bind_symbols={"A": "BTCUSDT", "B": "ETHUSDT"})
    assert order.index("portfolio") < order.index("risk")
    assert order.index("portfolio") < order.index("execution")
