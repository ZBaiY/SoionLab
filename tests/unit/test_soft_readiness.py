from __future__ import annotations

import logging

import pytest

from quant_engine.data.sentiment.sentiment_handler import SentimentDataHandler
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.contracts.execution.engine import ExecutionEngineProto
from quant_engine.contracts.feature import FeatureExtractorProto
from quant_engine.contracts.model import ModelBase
from quant_engine.contracts.decision import DecisionBase
from quant_engine.risk.engine import RiskEngine
from quant_engine.contracts.portfolio import PortfolioBase, PortfolioState


class DummyFeatureExtractor(FeatureExtractorProto):
    required_windows = {}
    warmup_steps = 1

    def update(self, timestamp: int | None = None) -> dict:
        return {}

    def warmup(self, *, anchor_ts: int) -> None:
        return None
    def set_interval(self, interval: str | None) -> None:
        return None
    def set_interval_ms(self, interval_ms: int | None) -> None:
        return None
    


class DummyModel(ModelBase):
    def predict(self, features: dict) -> float:
        return 0.0


class DummyDecision(DecisionBase):
    def decide(self, context: dict) -> float:
        return 0.0


class BullishDecision(DecisionBase):
    def decide(self, context: dict) -> float:
        return 1.0


class DummyExecutionEngine(ExecutionEngineProto):
    def execute(
        self,
        timestamp: int,
        target_position: float,
        portfolio_state: dict[str, object],
        primary_snapshots: dict[str, object] | None,
    ) -> list[dict]:
        return []


class SpyExecutionEngine(ExecutionEngineProto):
    def __init__(self) -> None:
        self.calls = 0

    def execute(
        self,
        timestamp: int,
        target_position: float,
        portfolio_state: dict[str, object],
        primary_snapshots: dict[str, object] | None,
    ) -> list[dict]:
        self.calls += 1
        return []


class DummyPortfolio(PortfolioBase):
    def __init__(self, symbol: str):
        super().__init__(symbol=symbol)

    def apply_fill(self, fill: dict) -> dict | None:
        return None

    def state(self) -> PortfolioState:
        return PortfolioState({"symbol": self.symbol})


class RichPortfolio(PortfolioBase):
    def __init__(self, symbol: str) -> None:
        super().__init__(symbol=symbol)

    def apply_fill(self, fill: dict) -> dict | None:
        return None

    def update_marks(self, market_snapshots: dict[str, object]) -> None:
        return None

    def state(self) -> PortfolioState:
        return PortfolioState(
            {
                "symbol": self.symbol,
                "cash": 1973.299691920285,
                "position": 31.885,
                "position_qty": 31.885,
                "position_lots": 31885,
                "qty_step": "0.001",
                "step_size": 0.001,
                "qty_mode": "LOTS",
                "min_qty": 0.0001,
                "min_notional": 1.0,
                "total_equity": 978738.0708259778,
            }
        )


class CloseOnlySnapshot:
    def __init__(self, close: float, data_ts: int) -> None:
        self._close = close
        self.data_ts = data_ts

    def get_attr(self, name: str):
        if name == "close":
            return self._close
        return None


def _build_engine() -> StrategyEngine:
    spec = EngineSpec.from_interval(
        mode=EngineMode.BACKTEST,
        interval="1m",
        symbol="BTCUSDT",
        universe={
            "soft_readiness": {
                "enabled": True,
                "domains": ["sentiment"],
                "max_staleness_ms": 60_000,
            }
        },
    )
    sentiment = SentimentDataHandler(symbol="BTCUSDT", interval="1m")
    return StrategyEngine(
        spec=spec,
        ohlcv_handlers={},
        orderbook_handlers={},
        option_chain_handlers={},
        iv_surface_handlers={},
        sentiment_handlers={"BTCUSDT": sentiment},
        trades_handlers={},
        option_trades_handlers={},
        feature_extractor=DummyFeatureExtractor(),
        models={"main": DummyModel(symbol="BTCUSDT")},
        decision=DummyDecision(),
        risk_manager=RiskEngine(rules=[], symbol="BTCUSDT"),
        execution_engine=DummyExecutionEngine(),
        portfolio_manager=DummyPortfolio(symbol="BTCUSDT"),
        guardrails=False,
    )


def test_soft_readiness_warns_when_missing(caplog: pytest.LogCaptureFixture) -> None:
    engine = _build_engine()
    engine._warmup_done = True
    engine._preload_done = True
    with caplog.at_level(logging.WARNING):
        engine.step(ts=1_700_000_000_000)
    assert any("soft_domain.not_ready" in rec.getMessage() for rec in caplog.records)


def test_soft_readiness_clamps_target_to_hold() -> None:
    engine = _build_engine()
    engine.decision = BullishDecision()
    engine._warmup_done = True
    engine._preload_done = True
    snap = engine.step(ts=1_700_000_000_000)
    assert float(snap.target_position) == 0.0


def test_hold_intent_skips_execution_for_score_to_target() -> None:
    spec = EngineSpec.from_interval(
        mode=EngineMode.BACKTEST,
        interval="15m",
        symbol="BTCUSDT",
        universe={},
    )
    execution = SpyExecutionEngine()
    engine = StrategyEngine(
        spec=spec,
        ohlcv_handlers={},
        orderbook_handlers={},
        option_chain_handlers={},
        iv_surface_handlers={},
        sentiment_handlers={},
        trades_handlers={},
        option_trades_handlers={},
        feature_extractor=DummyFeatureExtractor(),
        models={"main": DummyModel(symbol="BTCUSDT")},
        decision=DummyDecision(),
        risk_manager=RiskEngine(
            rules=[],
            symbol="BTCUSDT",
            risk_config={"shortable": False, "mapping": {"name": "score_to_target"}},
        ),
        execution_engine=execution,
        portfolio_manager=RichPortfolio(symbol="BTCUSDT"),
        guardrails=False,
    )
    engine._collect_market_data = lambda ts: {  # type: ignore[method-assign]
        "ohlcv": {"BTCUSDT": CloseOnlySnapshot(30633.9899995, data_ts=1687573799999)}
    }
    engine._warmup_done = True
    engine._preload_done = True

    snap = engine.step(ts=1687574699999)

    assert execution.calls == 0
    assert snap.fills == []
