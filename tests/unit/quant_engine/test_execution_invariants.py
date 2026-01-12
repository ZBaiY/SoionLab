from __future__ import annotations

import math
import re
from typing import Any, cast

from quant_engine.data.contracts.snapshot import Snapshot
from quant_engine.data.ohlcv.snapshot import OHLCVSnapshot
from quant_engine.execution.loader import ExecutionLoader
from quant_engine.portfolio.registry import build_portfolio
from quant_engine.strategy.registry import STRATEGY_REGISTRY


_SYMBOL_POOL = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]


def _collect_placeholders(obj: Any) -> set[str]:
    placeholders: set[str] = set()
    if isinstance(obj, str):
        placeholders.update(re.findall(r"{([^{}]+)}", obj))
        return placeholders
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(key, str):
                placeholders.update(re.findall(r"{([^{}]+)}", key))
            placeholders.update(_collect_placeholders(value))
        return placeholders
    if isinstance(obj, (list, tuple, set)):
        for item in obj:
            placeholders.update(_collect_placeholders(item))
        return placeholders
    return placeholders


def _placeholder_value(name: str, symbol_idx: int) -> str:
    upper = name.upper()
    lower = name.lower()
    if upper in {"A", "B", "C", "D"}:
        return _SYMBOL_POOL[min(symbol_idx, len(_SYMBOL_POOL) - 1)]
    if "rolling" in lower or "roll" in lower:
        return "5"
    if "window" in lower or "lookback" in lower or "period" in lower:
        return "14"
    if "symbol" in lower or "asset" in lower or "ticker" in lower:
        return _SYMBOL_POOL[0]
    return "1"


def _symbol_map_for_strategy(strategy_cls: type) -> dict[str, str]:
    placeholders: set[str] = set()
    for obj in (
        strategy_cls.UNIVERSE_TEMPLATE,
        strategy_cls.DATA,
        strategy_cls.FEATURES_USER,
        strategy_cls.MODEL_CFG,
        strategy_cls.DECISION_CFG,
        strategy_cls.RISK_CFG,
        strategy_cls.EXECUTION_CFG,
        strategy_cls.PORTFOLIO_CFG,
        strategy_cls.PRESETS,
    ):
        placeholders.update(_collect_placeholders(obj))

    symbol_map: dict[str, str] = {}
    symbol_idx = 0
    for name in sorted(placeholders):
        value = _placeholder_value(name, symbol_idx)
        symbol_map[name] = value
        if value in _SYMBOL_POOL and name.upper() in {"A", "B", "C", "D"}:
            symbol_idx += 1
    return symbol_map


def _strategy_configs() -> list:
    cfgs = []
    for strategy_cls in STRATEGY_REGISTRY.values():
        symbols = _symbol_map_for_strategy(strategy_cls)
        cfgs.append(strategy_cls.standardize(symbols=symbols))
    return cfgs


def _assert_finite(value: float) -> None:
    assert isinstance(value, (float, int))
    assert math.isfinite(float(value))


def test_execution_invariants() -> None:
    covered: set[tuple[str, str]] = set()

    for cfg in _strategy_configs():
        if cfg.execution is None:
            continue
        exec_cfg = cfg.execution
        symbol = str(cfg.symbol or _SYMBOL_POOL[0])
        portfolio_cfg = cfg.portfolio or {"type": "STANDARD", "params": {"initial_capital": 100000.0}}
        portfolio = build_portfolio(
            str(portfolio_cfg.get("type", "STANDARD")),
            symbol=symbol,
            **dict(portfolio_cfg.get("params", {})),
        )
        portfolio_state = portfolio.state().to_dict()
        portfolio_state.setdefault("slippage_bps", 10.0)
        portfolio_state.setdefault("fee_buffer", 0.0)

        snapshot = OHLCVSnapshot.from_bar_aligned(
            timestamp=1_700_000_000_000,
            bar={
                "data_ts": 1_700_000_000_000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1200.0,
            },
            symbol=symbol,
        )
        primary_snapshots = {"ohlcv": snapshot}

        engine = ExecutionLoader.from_config(symbol=symbol, cfg=exec_cfg)
        fills1 = engine.execute(
            timestamp=1_700_000_000_000,
            target_position=0.5,
            portfolio_state=portfolio_state,
            primary_snapshots=cast(dict[str, Snapshot], primary_snapshots),
        )
        fills2 = engine.execute(
            timestamp=1_700_000_000_000,
            target_position=0.5,
            portfolio_state=portfolio_state,
            primary_snapshots=cast(dict[str, Snapshot], primary_snapshots),
        )
        assert fills1 == fills2

        for fill in fills1:
            price = float(fill["fill_price"])
            qty = float(fill["filled_qty"])
            _assert_finite(price)
            _assert_finite(qty)
            assert price > 0.0
            assert qty > 0.0
            assert str(fill.get("side", "")).upper() == "BUY"

            before_cash = float(portfolio.state().to_dict().get("cash", 0.0))
            outcome = portfolio.apply_fill(fill)
            assert outcome is not None
            decision = outcome.get("execution_decision")
            if decision in {"ACCEPTED", "CLAMPED"}:
                assert float(portfolio.state().to_dict().get("cash", 0.0)) <= before_cash
                state = portfolio.state().to_dict()
                _assert_finite(state.get("cash", 0.0))
                _assert_finite(state.get("total_equity", 0.0))
                assert state.get("position_qty", 0.0) >= 0.0

        covered.add((exec_cfg["policy"]["type"], type(engine.policy).__name__))
        covered.add((exec_cfg["router"]["type"], type(engine.router).__name__))
        covered.add((exec_cfg["slippage"]["type"], type(engine.slippage).__name__))
        covered.add((exec_cfg["matching"]["type"], type(engine.matcher).__name__))

    covered_str = ", ".join([f"{etype}:{cls}" for etype, cls in sorted(covered)])
    print(f"covered implementations: {covered_str}")
