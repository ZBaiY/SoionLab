from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from quant_engine.execution.loader import ExecutionLoader
from quant_engine.execution.exchange.binance_client import BinanceSpotClient
from quant_engine.health.events import Action, ActionKind, ExecutionPermit, FaultEvent
from quant_engine.risk.rules_constraints import CashPositionConstraintRule, FractionalCashConstraintRule
from quant_engine.runtime.modes import EngineMode
from quant_engine.strategy.loader import StrategyLoader
from quant_engine.strategy.registry import get_strategy
from apps.run_code import realtime_app


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


class _CacheMissClient:
    def __init__(self) -> None:
        self.network_calls = 0

    def get_cached_symbol_filters(self, symbol: str):
        return None

    def get_symbol_filters(self, symbol: str, refresh: bool = False):
        self.network_calls += 1
        raise AssertionError("loader must not call network filter fetch")


class _CloseSnapshot:
    def __init__(self, close: float, data_ts: int = 1_700_000_000_000) -> None:
        self._close = float(close)
        self.data_ts = int(data_ts)

    def get_attr(self, name: str):
        if name == "close":
            return self._close
        return None


def _build_live_cfg_with_client(client: Any) -> dict[str, Any]:
    StrategyCls = get_strategy("EXAMPLE")
    cfg = StrategyCls.standardize(overrides={}, symbols={"A": "BTCUSDT", "B": "ETHUSDT"}).to_dict()
    execution_cfg = cfg.get("execution") if isinstance(cfg, dict) else {}
    assert isinstance(execution_cfg, dict)
    matching_cfg = execution_cfg.get("matching")
    assert isinstance(matching_cfg, dict)
    matching_cfg["type"] = "LIVE-BINANCE"
    params = matching_cfg.get("params")
    if not isinstance(params, dict):
        params = {}
    params["client"] = client
    matching_cfg["params"] = params
    execution_cfg["matching"] = matching_cfg
    cfg["execution"] = execution_cfg
    return cfg


def test_loader_live_binance_cache_miss_fails_without_network(monkeypatch):
    client = _CacheMissClient()
    cfg = _build_live_cfg_with_client(client)

    with pytest.raises(RuntimeError, match="filter metadata cache miss"):
        StrategyLoader.from_config(strategy=cfg, mode=EngineMode.REALTIME, overrides={})
    assert client.network_calls == 0


def test_realtime_preflight_populates_cache_and_syncs_constraints(monkeypatch, caplog):
    monkeypatch.setenv("BINANCE_ENV", "testnet")
    monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
    monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")

    calls = {"exchange_info": 0}

    def _api_exchange_info(self, method, path, **kwargs):
        if method == "GET" and path == "/api/v3/exchangeInfo":
            calls["exchange_info"] += 1
            return {
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "0.01", "minQty": "0.01"},
                            {"filterType": "MIN_NOTIONAL", "minNotional": "50"},
                        ],
                    }
                ]
            }
        raise AssertionError(f"unexpected api call {method} {path}")

    monkeypatch.setattr(BinanceSpotClient, "api", _api_exchange_info)

    with caplog.at_level("INFO"):
        engine, _driver_cfg, _plan = realtime_app.build_realtime_engine(
            strategy_name="EXAMPLE",
            bind_symbols={"A": "BTCUSDT", "B": "ETHUSDT"},
            overrides={
                "execution": {
                    "policy": {"type": "IMMEDIATE"},
                    "router": {"type": "SIMPLE"},
                    "slippage": {"type": "LINEAR"},
                    "matching": {"type": "LIVE-BINANCE", "params": {"run_tag": "smoke"}},
                }
            },
        )

    # A1: one explicit startup metadata fetch, loader path remains cache-only.
    assert calls["exchange_info"] == 1

    # B1: canonical synced values are projected into portfolio state.
    state = engine.portfolio.state().to_dict()
    assert abs(float(state.get("step_size", 0.0)) - 0.01) < 1e-12
    assert abs(float(state.get("min_qty", 0.0)) - 0.01) < 1e-12
    assert abs(float(state.get("min_notional", 0.0)) - 50.0) < 1e-12

    # Policy follows synced min_notional (20 < 50 => no order).
    equity = float(state.get("total_equity", 0.0))
    target_position = (20.0 / equity) if equity > 0 else 0.0
    market_data = {"ohlcv": _CloseSnapshot(100.0)}
    assert engine.execution_engine.policy.generate(target_position, state, market_data) == []

    # Risk cash rule follows same synced min_notional from portfolio snapshot.
    cash_rule = next(
        (
            r
            for r in engine.risk_manager.rules
            if isinstance(r, (CashPositionConstraintRule, FractionalCashConstraintRule))
        ),
        None,
    )
    assert cash_rule is not None
    adjusted = cash_rule.adjust(
        target_position,
        {
            "timestamp": 1_700_000_000_000,
            "portfolio": state,
            "primary_snapshots": {"ohlcv": _CloseSnapshot(100.0)},
        },
    )
    assert abs(float(adjusted) - 0.0) < 1e-12

    assert any(rec.message == "loader.execution_constraints.synced" for rec in caplog.records)
