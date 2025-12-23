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
    UNIVERSE_TEMPLATE = {
        "primary": "{A}",
        "secondary": {"{B}"},
    }

    DATA = {
        "primary": {
            "ohlcv": {"$ref": "OHLCV_1M_30D"},
            "orderbook": {"$ref": "ORDERBOOK_L2_10_100MS"},
            "option_chain": {"$ref": "OPTION_CHAIN_5M"},
            "iv_surface": {"$ref": "IV_SURFACE_5M"},
            "sentiment": {"$ref": "SENTIMENT_BASIC_5M"},
        },
        "secondary": {
            "{B}": {
                "ohlcv": {"$ref": "OHLCV_1M_30D"},
            }
        },
    }
    REQUIRED_DATA = {"ohlcv", "orderbook", "option_chain", "iv_surface", "sentiment"}
    FEATURES_USER = [
        {
            "name": "SPREAD_MODEL_{A}^{B}",
            "type": "SPREAD",
            "symbol": "{A}",
            "params": {"ref": "{B}"},
        },
        {
            "name": "ZSCORE_MODEL_{A}^{B}",
            "type": "ZSCORE",
            "symbol": "{A}",
            "params": {"ref": "{B}", "lookback": 120},
        },
        {
            "name": "ATR_RISK_{A}",
            "type": "ATR",
            "symbol": "{A}",
            "params": {"window": 14},
        },
    ]
    MODEL_CFG = {
        "type": "PAIR_ZSCORE",
        "params": {"zscore_feature": "ZSCORE_MODEL_{A}^{B}"},
    }
    DECISION_CFG = {
        "type": "ZSCORE_THRESHOLD",
        "params": {
            "zscore_feature": "ZSCORE_MODEL_{A}^{B}",
            "enter": 2.0,
            "exit": 0.5,
        },
    }
    RISK_CFG = {
        "rules": {
            "ATR_SIZER": {
                "params": {"atr_feature": "ATR_RISK_{A}"}
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

class RSIADXSidewaysStrategy(StrategyBase):

    STRATEGY_NAME = "RSI_ADX_SIDEWAYS"

    # B-style, but single-symbol
    UNIVERSE_TEMPLATE = {
        "primary": "{A}",
    }

    DATA = {
        "primary": {
            "ohlcv": {"$ref": "OHLCV_1M_30D"},
        }
    }

    REQUIRED_DATA = {"ohlcv"}

    FEATURES_USER = [
        {
            "name": "RSI_MODEL_{A}",
            "type": "RSI",
            "symbol": "{A}",
            "params": {"window": 14},
        },
        {
            "name": "ADX_MODEL_{A}",
            "type": "ADX",
            "symbol": "{A}",
            "params": {"window": 14},
        },
        {
            "name": "RSI_MEAN_MODEL_{A}",
            "type": "RSI_ROLLING_MEAN",
            "symbol": "{A}",
            "params": {
                "feature": "RSI_{A}",
                "window": "{window}",
            },
        },
        {
            "name": "RSI_STD_MODEL_{A}",
            "type": "RSI_ROLLING_STD",
            "symbol": "{A}",
            "params": {
                "feature": "RSI_{A}",
                "window": "{window}",
            },
        },
    ]

    # no model — rule-based decision
    MODEL_CFG = None

    DECISION_CFG = {
        "type": "RSI_DYNAMIC_BAND",
        "params": {
            "rsi": "RSI_MODEL_{A}",
            "rsi_mean": "RSI_MEAN_MODEL_{A}",
            "rsi_std": "RSI_STD_MODEL_{A}",
            "adx": "ADX_MODEL_{A}",
            "adx_threshold": 25,
            "variance_factor": 1.8,
            "mae": 0.0,
        },
    }

    RISK_CFG = {
        "rules": {
            "FULL_ALLOCATION": {}
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
        "params": {"initial_capital": 1000},
    }