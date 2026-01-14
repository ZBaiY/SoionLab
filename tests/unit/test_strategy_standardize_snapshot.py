from __future__ import annotations

import importlib

from apps.strategy.strategies import RSIADXSidewaysStrategy


def test_apps_strategy_importable() -> None:
    importlib.import_module("apps.strategy")


def test_strategy_standardize_snapshot() -> None:
    cfg = RSIADXSidewaysStrategy.standardize(
        {"presets": {"LOCAL": {"interval": "5m"}}},
        symbols={
            "A": "BTCUSDT",
            "window_RSI": "14",
            "window_ADX": "14",
            "window_RSI_rolling": "5",
        },
    )
    assert cfg.to_dict() == {
        "data": {
            "primary": {
                "ohlcv": {
                    "bootstrap": {"lookback": "180d"},
                    "cache": {"max_bars": 10000},
                    "columns": ["open", "high", "low", "close", "volume"],
                    "interval": "15m",
                    "interval_ms": 900000,
                }
            }
        },
        "decision": {
            "params": {
                "adx": "ADX_DECISION_BTCUSDT",
                "adx_threshold": 25,
                "mae": 0.0,
                "rsi": "RSI_DECISION_BTCUSDT",
                "rsi_mean": "RSI-MEAN_DECISION_BTCUSDT",
                "rsi_std": "RSI-STD_DECISION_BTCUSDT",
                "variance_factor": 1.8,
            },
            "type": "RSI-DYNAMIC-BAND",
        },
        "execution": {
            "matching": {"type": "SIMULATED"},
            "policy": {"type": "IMMEDIATE"},
            "router": {"type": "SIMPLE"},
            "slippage": {"type": "LINEAR"},
        },
        "features_user": [
            {
                "name": "RSI_DECISION_BTCUSDT",
                "params": {"window": "14"},
                "symbol": "BTCUSDT",
                "type": "RSI",
            },
            {
                "name": "ADX_DECISION_BTCUSDT",
                "params": {"window": "14"},
                "symbol": "BTCUSDT",
                "type": "ADX",
            },
            {
                "name": "RSI-MEAN_DECISION_BTCUSDT",
                "params": {"window_rolling": "5", "window_rsi": "14"},
                "symbol": "BTCUSDT",
                "type": "RSI-MEAN",
            },
            {
                "name": "RSI-STD_DECISION_BTCUSDT",
                "params": {"window_rolling": "5", "window_rsi": "14"},
                "symbol": "BTCUSDT",
                "type": "RSI-STD",
            },
        ],
        "interval": "15m",
        "interval_ms": 900000,
        "model": None,
        "portfolio": {"params": {"initial_capital": 1000000}, "type": "STANDARD"},
        "required_data": ["ohlcv"],
        "risk": {
            "rules": {
                "CASH-POSITION-CONSTRAINT": {
                    "params": {"fee_rate": 0.001, "min_notional": 10.0, "slippage_bound_bps": 10}
                },
                "FULL-ALLOCATION": {},
            },
            "shortable": False,
        },
        "symbol": "BTCUSDT",
        "universe": {"primary": "BTCUSDT"},
    }
