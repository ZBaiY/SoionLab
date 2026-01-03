"""
Strategy registry.

This module provides:
- a global registry for Strategy classes
- a decorator for registration
- a factory for instantiation

Strategy classes are declarative and defined elsewhere.
"""

from __future__ import annotations
from typing import Dict, Type

from quant_engine.strategy.base import StrategyBase


# =====================================================================
# Registry
# =====================================================================

STRATEGY_REGISTRY: Dict[str, Type[StrategyBase]] = {}


def register_strategy(name: str):
    """
    Register a Strategy class under a string key.

    Example
    -------
    @register_strategy("VOL_ARB")
    class VolArbStrategy(StrategyBase):
        REQUIRED_DATA = {"ohlcv", "option_chain", "iv_surface"}
        REQUIRED_FEATURES = {"atm_iv", "skew"}
    """
    def decorator(cls: Type[StrategyBase]) -> Type[StrategyBase]:
        if name in STRATEGY_REGISTRY:
            raise KeyError(f"Strategy '{name}' already registered")

        if not issubclass(cls, StrategyBase):
            raise TypeError("Only StrategyBase subclasses can be registered")

        cls.STRATEGY_NAME = name
        STRATEGY_REGISTRY[name] = cls
        return cls

    return decorator


from quant_engine.strategy.strategies import *  # noqa: F403,F401


def get_strategy(name: str) -> Type[StrategyBase]:
    """
    Retrieve a registered Strategy class by name.
    """
    try:
        return STRATEGY_REGISTRY[name]
    except KeyError:
        raise KeyError(
            f"Unknown strategy '{name}'. "
            f"Available strategies: {list(STRATEGY_REGISTRY.keys())}"
        )
