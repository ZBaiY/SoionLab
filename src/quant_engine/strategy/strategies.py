"""
Concrete strategy definitions.

This module contains registered Strategy implementations that declare
their data and feature dependencies.
"""

from quant_engine.strategy.base import StrategyBase
from quant_engine.strategy.registry import register_strategy


@register_strategy("EXAMPLE")
class ExampleStrategy(StrategyBase):

    STRATEGY_NAME = "EXAMPLE"
    SYMBOL = "BTCUSDT"

    DATA = {
        "primary": {
            "ohlcv": {"$ref": "BINANCE_OHLCV_1M_30D"},
            "orderbook": {"$ref": "BINANCE_ORDERBOOK_L2_10_100MS"},
            "option_chain": {"$ref": "DERIBIT_OPTION_CHAIN_5M"},
            "iv_surface": {"$ref": "DERIBIT_IV_SURFACE_5M"},
            "sentiment": {"$ref": "SENTIMENT_BASIC_5M"},
        },
        "secondary": {
            "ETHUSDT": {"ohlcv": {"$ref": "BINANCE_OHLCV_1M_30D"},}
        },
    }
    REQUIRED_DATA = {"ohlcv", "orderbook", "option_chain", "iv_surface", "sentiment"}
    FEATURES_USER = [
        {
            "name": "SPREAD_MODEL_BTCUSDT^ETHUSDT",
            "type": "SPREAD",
            "symbol": "BTCUSDT",
            "params": {"ref": "ETHUSDT"},
        },
        {
            "name": "ZSCORE_MODEL_BTCUSDT^ETHUSDT",
            "type": "ZSCORE",
            "symbol": "BTCUSDT",
            "params": {"ref": "ETHUSDT", "lookback": 120},
        },
        {
            "name": "ATR_RISK_BTCUSDT",
            "type": "ATR",
            "symbol": "BTCUSDT",
            "params": {"window": 14},
        },
    ]
    MODEL_CFG = {
        "type": "PAIR_ZSCORE",
        "params": {"zscore_feature": "ZSCORE_MODEL_BTCUSDT^ETHUSDT",},
    }
    DECISION_CFG = {
        "type": "ZSCORE_THRESHOLD",
        "params": {
            "zscore_feature": "ZSCORE_MODEL_BTCUSDT^ETHUSDT",
            "enter": 2.0,
            "exit": 0.5,
        },
    }
    RISK_CFG = {
        "rules": {
            "ATR_SIZER": {
                "params": {"atr_feature": "ATR_RISK_BTCUSDT",}
            },
            "EXPOSURE_LIMIT": {
                "params": {"limit": 2.0},
            },
        }
    }
    EXECUTION_CFG = {
        "policy": {"type": "IMMEDIATE"},
        "router": {"type": "SIMPLE"},
        "slippage": {"type": "LINEAR"},
        "matching": {"type": "SIMULATED"},
    }
    PORTFOLIO_CFG = {
        "type": "STANDARD",
        "params": {"initial_capital": 10000,},
    }
