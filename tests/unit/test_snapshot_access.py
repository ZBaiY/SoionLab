from __future__ import annotations

import dataclasses

from quant_engine.data.contracts.snapshot import MarketInfo
from quant_engine.data.orderbook.snapshot import OrderbookSnapshot
from quant_engine.execution.engine import ExecutionEngine
from quant_engine.execution.matching.simulated import SimulatedMatchingEngine
from quant_engine.execution.policy.immediate import ImmediatePolicy
from quant_engine.execution.router.l1_aware import L1AwareRouter
from quant_engine.execution.slippage.linear import LinearSlippage


def test_execution_uses_snapshot_attrs(monkeypatch) -> None:
    """Snapshots are frozen dataclasses; access via attributes/get_attr; context uses primary_snapshots and market_snapshots."""
    def _boom(*args, **kwargs):
        raise AssertionError("Snapshot serialization should not be invoked in execution")

    monkeypatch.setattr(dataclasses, "asdict", _boom)
    monkeypatch.setattr(OrderbookSnapshot, "to_dict", _boom)

    market = MarketInfo(
        venue="test",
        asset_class="crypto",
        timezone="UTC",
        calendar="24x7",
        session="24x7",
        status="open",
        gap_type=None,
    )
    snap = OrderbookSnapshot(
        timestamp=1_700_000_000_000,
        data_ts=1_700_000_000_000,
        latency=0,
        symbol="BTCUSDT",
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

    engine = ExecutionEngine(
        ImmediatePolicy(symbol="BTCUSDT"),
        L1AwareRouter(symbol="BTCUSDT"),
        LinearSlippage(symbol="BTCUSDT", impact=0.001),
        SimulatedMatchingEngine(symbol="BTCUSDT"),
    )

    fills = engine.execute(
        timestamp=1_700_000_000_000,
        target_position=1.0,
        portfolio_state={"position": 0.0},
        primary_snapshots={"orderbook": snap},
    )

    assert fills
