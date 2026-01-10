from __future__ import annotations

import pytest

from quant_engine.decision.threshold import ThresholdDecision
from quant_engine.execution.engine import ExecutionEngine
from quant_engine.execution.matching.registry import build_matching
from quant_engine.execution.policy.registry import build_policy
from quant_engine.execution.router.registry import build_router
from quant_engine.execution.slippage.registry import build_slippage
from quant_engine.data.contracts.snapshot import MarketInfo
from quant_engine.data.orderbook.snapshot import OrderbookSnapshot
from quant_engine.portfolio.registry import build_portfolio
from quant_engine.risk.engine import RiskEngine
from quant_engine.risk.rules_exposure import ExposureLimitRule


@pytest.mark.integration
def test_execution_chain_real_modules() -> None:
    symbol = "BTCUSDT"
    decision = ThresholdDecision(threshold=0.5)
    risk_engine = RiskEngine([ExposureLimitRule(symbol=symbol, limit=2.0)], symbol=symbol)
    policy = build_policy("IMMEDIATE", symbol=symbol)
    router = build_router("SIMPLE", symbol=symbol)
    slippage = build_slippage("LINEAR", symbol=symbol, impact=0.001)
    matcher = build_matching("SIMULATED", symbol=symbol)
    execution_engine = ExecutionEngine(policy, router, slippage, matcher)
    portfolio = build_portfolio("STANDARD", symbol=symbol, initial_capital=10000.0)

    context = {"models": {"main": 1.0}}
    decision_score = decision.decide(context)
    target_position = risk_engine.adjust(decision_score, context)

    market = MarketInfo(
        venue="test",
        asset_class="crypto",
        timezone="UTC",
        calendar="24x7",
        session="24x7",
        status="open",
        gap_type=None,
    )
    primary_snapshots = {
        "orderbook": OrderbookSnapshot(
            timestamp=1_700_000_000_000,
            data_ts=1_700_000_000_000,
            latency=0,
            symbol=symbol,
            market=market,
            domain="orderbook",
            schema_version=1,
            best_bid=100.0,
            best_bid_size=1.0,
            best_ask=101.0,
            best_ask_size=1.0,
            bids=[],
            asks=[],
        )
    }
    fills = execution_engine.execute(
        target_position=target_position,
        portfolio_state=portfolio.state().to_dict(),
        primary_snapshots=primary_snapshots,
        timestamp=1_700_000_000_000,
    )

    assert fills
    for fill in fills:
        portfolio.apply_fill(fill)

    state = portfolio.state().to_dict()
    assert state["positions"][symbol]["qty"] != 0.0
    assert state["cash"] < 10000.0
    assert fills[0]["slippage"] == pytest.approx(0.001)
