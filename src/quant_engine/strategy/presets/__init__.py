"""
Strategy global presets registry data.
"""

from __future__ import annotations

from typing import Any, Dict

from .ohlcv import OHLCV_PRESETS
from .option_chain import OPTION_CHAIN_PRESETS
from .sentiment import SENTIMENT_PRESETS

_ORDERBOOK_PRESETS: Dict[str, Any] = {
    "ORDERBOOK_L2_10_100MS": {
        "depth": 10,
        "aggregation": "L2",
        "interval": "100ms",
        "bootstrap": {"lookback": "1d"},
        "cache": {"max_bars": 10000},
    },
    "ORDERBOOK_L2_20_250MS": {
        "depth": 20,
        "aggregation": "L2",
        "interval": "250ms",
        "bootstrap": {"lookback": "1d"},
        "cache": {"max_bars": 10000},
    },
}

_IV_SURFACE_PRESETS: Dict[str, Any] = {
    "IV_SURFACE_5M": {
        "interval": "5m",
        "calibrator": "SSVI",
        "bootstrap": {"lookback": "30d"},
        "cache": {"max_bars": 10000},
    },
    "IV_SURFACE_5M_FETCHED": {
        "interval": "5m",
        "calibrator": "FETCHED",
        "bootstrap": {"lookback": "30d"},
        "cache": {"max_bars": 10000},
    },
    "IV_SURFACE_1M": {
        "interval": "1m",
        "calibrator": "SSVI",
        "bootstrap": {"lookback": "30d"},
        "cache": {"max_bars": 10000},
    },
}

GLOBAL_PRESETS: Dict[str, Any] = {
    **OHLCV_PRESETS,
    **_ORDERBOOK_PRESETS,
    **OPTION_CHAIN_PRESETS,
    **_IV_SURFACE_PRESETS,
    **SENTIMENT_PRESETS,
}

__all__ = ["GLOBAL_PRESETS", "OHLCV_PRESETS", "OPTION_CHAIN_PRESETS", "SENTIMENT_PRESETS"]
