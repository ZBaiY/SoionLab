from __future__ import annotations

from apps.run_backtest import build_backtest_engine
from apps.run_mock import build_mock_engine
from apps.run_realtime import build_realtime_engine
from quant_engine.runtime.modes import EngineMode


def test_build_backtest_engine_returns_plan() -> None:
    engine, driver_cfg, plan = build_backtest_engine(
        strategy_name="RSI_ADX_SIDEWAYS",
        bind_symbols={"A": "BTCUSDT"},
        require_local_data=False,
    )
    assert engine.spec.mode == EngineMode.BACKTEST
    assert "start_ts" in driver_cfg and "end_ts" in driver_cfg
    assert isinstance(plan, list)
    if plan:
        assert callable(plan[0]["build_worker"])


def test_build_realtime_engine_returns_plan() -> None:
    engine, driver_cfg, plan = build_realtime_engine(
        strategy_name="RSI_ADX_SIDEWAYS",
        bind_symbols={"A": "BTCUSDT"},
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
