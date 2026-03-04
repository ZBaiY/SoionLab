from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from quant_engine.execution.loader import ExecutionLoader
from quant_engine.execution.exchange.binance_client import BinanceSpotClient
from quant_engine.health.events import Action, ActionKind, ExecutionPermit, FaultEvent


class _MiniOrderbook:
    def __init__(self, *, bid: float, ask: float, data_ts: int = 1_700_000_000_000) -> None:
        self.best_bid = float(bid)
        self.best_ask = float(ask)
        self.data_ts = int(data_ts)

    def get_attr(self, name: str):
        return getattr(self, name, None)


@dataclass
class _SpyHealth:
    permit: ExecutionPermit
    events: list[FaultEvent] = field(default_factory=list)

    def execution_permit(self) -> ExecutionPermit:
        return self.permit

    def report(self, event: FaultEvent) -> Action:
        self.events.append(event)
        return Action(kind=ActionKind.CONTINUE, execution_permit=self.permit)


def _exec_cfg() -> dict[str, Any]:
    return {
        "policy": {"type": "IMMEDIATE", "params": {}},
        "router": {"type": "SIMPLE", "params": {}},
        "slippage": {"type": "LINEAR", "params": {"impact": 0.0}},
        "matching": {"type": "LIVE-BINANCE", "params": {"run_tag": "smoke"}},
    }


def _portfolio_state() -> dict[str, Any]:
    return {
        "cash": 10_000.0,
        "qty_step": 0.001,
        "min_qty": 0.001,
        "min_notional": 5.0,
        "position_qty": 0.0,
        "total_equity": 10_000.0,
    }


def _market_data() -> dict[str, Any]:
    return {"orderbook": _MiniOrderbook(bid=100.0, ask=101.0)}


def test_loader_live_binance_block_permit_prevents_submission(monkeypatch):
    monkeypatch.setenv("BINANCE_ENV", "testnet")
    monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
    monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")

    calls = {"api": 0}

    def _api_no_network(self, method, path, **kwargs):
        calls["api"] += 1
        raise AssertionError("network call")

    monkeypatch.setattr(BinanceSpotClient, "sync_time", lambda self: {"skew_ms": 0})
    monkeypatch.setattr(BinanceSpotClient, "api", _api_no_network)

    health = _SpyHealth(permit=ExecutionPermit.BLOCK)
    engine = ExecutionLoader.from_config(symbol="BTCUSDT", cfg=_exec_cfg(), health=health)

    fills = engine.execute(
        timestamp=1_700_000_000_000,
        target_position=0.3,
        portfolio_state=_portfolio_state(),
        primary_snapshots=_market_data(),
    )
    assert fills == []
    assert calls["api"] == 0, "no Binance API submission should occur under BLOCK permit"


def test_loader_live_binance_mainnet_guard_blocks_without_confirm_and_reports_health(monkeypatch):
    monkeypatch.setenv("BINANCE_ENV", "mainnet")
    monkeypatch.setenv("BINANCE_MAINNET_API_KEY", "mk")
    monkeypatch.setenv("BINANCE_MAINNET_API_SECRET", "ms")
    monkeypatch.setenv("BINANCE_MAINNET_CONFIRM", "NO")

    calls = {"api": 0}

    def _api_no_network(self, method, path, **kwargs):
        calls["api"] += 1
        raise AssertionError("network call")

    monkeypatch.setattr(BinanceSpotClient, "sync_time", lambda self: {"skew_ms": 0})
    monkeypatch.setattr(BinanceSpotClient, "api", _api_no_network)

    health = _SpyHealth(permit=ExecutionPermit.FULL)
    engine = ExecutionLoader.from_config(symbol="BTCUSDT", cfg=_exec_cfg(), health=health)

    fills = engine.execute(
        timestamp=1_700_000_000_000,
        target_position=0.3,
        portfolio_state=_portfolio_state(),
        primary_snapshots=_market_data(),
    )
    assert fills == []
    assert calls["api"] == 0, "mainnet guard should block before any HTTP call"
    assert health.events, "expected health fault event for mainnet guard block"
    evt = health.events[-1]
    assert evt.domain == "execution/binance"
    assert evt.source == "execution.binance.mainnet_guard"
    assert evt.exc_msg is not None and "BINANCE_MAINNET_CONFIRM=YES" in evt.exc_msg
