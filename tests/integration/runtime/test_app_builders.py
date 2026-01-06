from __future__ import annotations

from pathlib import Path

from apps.run_backtest import build_backtest_engine
from apps.run_mock import build_mock_engine
from apps.run_realtime import build_realtime_engine
from quant_engine.runtime.modes import EngineMode


def test_build_backtest_engine_returns_plan() -> None:
    engine, driver_cfg, plan = build_backtest_engine(
        strategy_name="RSI-ADX-SIDEWAYS",
        bind_symbols={"A": "BTCUSDT", 'window_RSI' : '14', 'window_ADX': '14', 'window_RSI_rolling': '5'},
        require_local_data=False,
    )
    assert engine.spec.mode == EngineMode.BACKTEST
    assert "start_ts" in driver_cfg and "end_ts" in driver_cfg
    assert isinstance(plan, list)
    if plan:
        assert callable(plan[0]["build_worker"])


def test_build_realtime_engine_returns_plan() -> None:
    engine, driver_cfg, plan = build_realtime_engine(
        strategy_name="RSI-ADX-SIDEWAYS",
        bind_symbols={"A": "BTCUSDT", 'window_RSI' : '14', 'window_ADX': '14', 'window_RSI_rolling': '5'},
    )
    assert engine.spec.mode == EngineMode.REALTIME
    assert isinstance(driver_cfg, dict)
    assert isinstance(plan, list)
    if plan:
        assert callable(plan[0]["build_worker"])
        assert callable(plan[0]["emit"])


def test_build_mock_engine_returns_empty_plan() -> None:
    engine, driver_cfg, plan = build_mock_engine(timestamps=[1], ticks=[])
    assert engine.spec.mode == EngineMode.MOCK
    assert driver_cfg["timestamps"] == [1]
    assert plan == []


def test_backtest_plan_detects_local_data() -> None:
    data_root = Path(__file__).resolve().parents[2] / "resources"
    _, _, plan = build_backtest_engine(
        strategy_name="RSI-ADX-SIDEWAYS",
        bind_symbols={
            "A": "BTCUSDT",
            "window_RSI": "14",
            "window_ADX": "14",
            "window_RSI_rolling": "5",
        },
        start_ts=1704067200000,
        end_ts=1704153600000,
        data_root=data_root,
        require_local_data=True,
    )
    assert any(entry.get("has_local_data") for entry in plan)


def test_backtest_plan_normalizes_option_chain_asset() -> None:
    data_root = Path(__file__).resolve().parents[2] / "resources"
    _, _, plan = build_backtest_engine(
        strategy_name="EXAMPLE",
        bind_symbols={"A": "BTCUSDT", "B": "ETHUSDT"},
        start_ts=1704067200000,
        end_ts=1704153600000,
        data_root=data_root,
        require_local_data=True,
    )
    matches = [
        entry
        for entry in plan
        if entry.get("domain") == "option_chain" and entry.get("asset") == "BTC"
    ]
    assert matches
    assert any(entry.get("has_local_data") for entry in matches)
    assert any(
        "option_chain" in p.parts and "BTC" in p.parts
        for entry in matches
        for p in entry.get("paths", [])
    )
