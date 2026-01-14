"""
Concrete strategy definitions.

This module contains registered Strategy implementations that declare
their data and feature dependencies.
Apps/strategy is the user-facing strategy interface.
"""

from quant_engine.strategy.base import StrategyBase
from quant_engine.strategy.registry import register_strategy

@register_strategy("EXAMPLE")
class ExampleStrategy(StrategyBase):

    STRATEGY_NAME = "EXAMPLE"
    INTERVAL = "30m"
    UNIVERSE_TEMPLATE = {
        "primary": "{A}",
        "secondary": {"{B}"},
        "soft_readiness": { ## New field to specify data domains that are "soft" requirements, i.e. non-blocking if missing
            "enabled": False,
            "domains": ["orderbook", "option_chain", "iv_surface", "sentiment"],
            "max_staleness_ms": 300000,
        },
    }

    DATA = {
        "primary": {
            "ohlcv": {"$ref": "OHLCV_15M_180D"},
            "option_chain": {"$ref": "OPTION_CHAIN_5M"},
            # "orderbook": {"$ref": "ORDERBOOK_L2_10_100MS"},
            # "iv_surface": {"$ref": "IV_SURFACE_5M"},
            # "sentiment": {"$ref": "SENTIMENT_BASIC_5M"},
        },
        "secondary": {
            "{B}": {
                "ohlcv": {"$ref": "OHLCV_15M_180D"},
            }
        },
    }
    REQUIRED_DATA = {"ohlcv", "option_chain"} # "orderbook", "iv_surface", "sentiment"
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
        "type": "PAIR-ZSCORE",
        "params": {"zscore_feature": "ZSCORE_MODEL_{A}^{B}", "secondary": "{B}"},
    }
    DECISION_CFG = {
        "type": "ZSCORE-THRESHOLD",
        "params": {
            "zscore_feature": "ZSCORE_MODEL_{A}^{B}",
            "enter": 2.0,
            "exit": 0.5,
        },
    }
    RISK_CFG = {
        "shortable": False,
        "rules": {
            "ATR-SIZER": {
                "params": {"atr_feature": "ATR_RISK_{A}"}
            },
            "EXPOSURE-LIMIT": {
                "params": {"limit": 2.0},
            },
            "CASH-POSITION-CONSTRAINT": {
                "params": {
                    "fee_rate": 0.001,
                    "slippage_bound_bps": 10,  # 10 bps = 0.1%
                    # "min_qty": 1,
                    "min_notional": 10.0,
                },
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
        "params": {"initial_capital": 1000000,},
    }

@register_strategy("RSI-ADX-SIDEWAYS")
class RSIADXSidewaysStrategy(StrategyBase):

    STRATEGY_NAME = "RSI-ADX-SIDEWAYS"
    INTERVAL = "15m"
    # B-style, but single-symbol
    UNIVERSE_TEMPLATE = {
        "primary": "{A}",
    }

    DATA = {
        "primary": {
            "ohlcv": {"$ref": "OHLCV_15M_180D"},
        }
    }

    REQUIRED_DATA = {"ohlcv"}

    FEATURES_USER = [
        {
            "name": "RSI_DECISION_{A}",
            "type": "RSI",
            "symbol": "{A}",
            "params": {"window": '{window_RSI}'},
        },
        {
            "name": "ADX_DECISION_{A}",
            "type": "ADX",
            "symbol": "{A}",
            "params": {"window": '{window_ADX}'},
        },
        {
            "name": "RSI-MEAN_DECISION_{A}",
            "type": "RSI-MEAN",
            "symbol": "{A}",
            "params": {
                "window_rsi": "{window_RSI}",
                "window_rolling": "{window_RSI_rolling}",
            },
        },
        {
            "name": "RSI-STD_DECISION_{A}",
            "type": "RSI-STD",
            "symbol": "{A}",
            "params": {
                "window_rsi": "{window_RSI}",
                "window_rolling": "{window_RSI_rolling}",
            },
        },
    ]

    # no model — rule-based decision
    MODEL_CFG = None

    DECISION_CFG = {
        "type": "RSI-DYNAMIC-BAND",
        "params": {
            "rsi": "RSI_DECISION_{A}",
            "rsi_mean": "RSI-MEAN_DECISION_{A}",
            "rsi_std": "RSI-STD_DECISION_{A}",
            "adx": "ADX_DECISION_{A}",
            "adx_threshold": 25,
            "variance_factor": 1.8,
            "mae": 0.0,
        },
    }

    RISK_CFG = {
        "shortable": False,
        "rules": {
            "FULL-ALLOCATION": {},
            "CASH-POSITION-CONSTRAINT": {
                "params": {
                    "fee_rate": 0.001,
                    "slippage_bound_bps": 10,
                    # "min_qty": 1,
                    "min_notional": 10.0,
                },
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
        "params": {"initial_capital": 1000000},
    }

@register_strategy("RSI-ADX-SIDEWAYS-FRACTIONAL")
class RSIADXSidewaysStrategyFractional(StrategyBase):

    STRATEGY_NAME = "RSI-ADX-SIDEWAYS-FRACTIONAL"
    INTERVAL = "15m"
    # B-style, but single-symbol
    UNIVERSE_TEMPLATE = {
        "primary": "{A}",
    }

    DATA = {
        "primary": {
            "ohlcv": {"$ref": "OHLCV_15M_180D"},
        }
    }

    REQUIRED_DATA = {"ohlcv"}

    FEATURES_USER = [
        {
            "name": "RSI_DECISION_{A}",
            "type": "RSI",
            "symbol": "{A}",
            "params": {"window": '{window_RSI}'},
        },
        {
            "name": "ADX_DECISION_{A}",
            "type": "ADX",
            "symbol": "{A}",
            "params": {"window": '{window_ADX}'},
        },
        {
            "name": "RSI-MEAN_DECISION_{A}",
            "type": "RSI-MEAN",
            "symbol": "{A}",
            "params": {
                "window_rsi": "{window_RSI}",
                "window_rolling": "{window_RSI_rolling}",
            },
        },
        {
            "name": "RSI-STD_DECISION_{A}",
            "type": "RSI-STD",
            "symbol": "{A}",
            "params": {
                "window_rsi": "{window_RSI}",
                "window_rolling": "{window_RSI_rolling}",
            },
        },
    ]

    # no model — rule-based decision
    MODEL_CFG = None

    DECISION_CFG = {
        "type": "RSI-DYNAMIC-BAND",
        "params": {
            "rsi": "RSI_DECISION_{A}",
            "rsi_mean": "RSI-MEAN_DECISION_{A}",
            "rsi_std": "RSI-STD_DECISION_{A}",
            "adx": "ADX_DECISION_{A}",
            "adx_threshold": 25,
            "variance_factor": 1.8,
            "mae": 0.0,
        },
    }

    RISK_CFG = {
        "shortable": False,
        "rules": {
            "FULL-ALLOCATION": {},
            "FRACTIONAL-CASH-CONSTRAINT": {
                "params": {
                    "fee_rate": 0.001,
                    "slippage_bound_bps": 10,
                    # "min_qty": 1,
                    "min_notional": 10.0,
                },
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
        "type": "FRACTIONAL",
        "params": {"initial_capital": 1000000, "step_size": 0.001},
    }
