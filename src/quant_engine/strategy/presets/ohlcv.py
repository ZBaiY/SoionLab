"""
Global OHLCV presets.
"""

from __future__ import annotations

from typing import Any, Dict

OHLCV_PRESETS: Dict[str, Any] = {
    "OHLCV_1M_30D": {
        "interval": "1m",
        "bootstrap": {"lookback": "30d"},
        "cache": {"max_bars": 10000},
        "columns": ["open", "high", "low", "close", "volume"],
    },
    "OHLCV_15M_180D": {
        "interval": "15m",
        "bootstrap": {"lookback": "180d"},
        "cache": {"max_bars": 10000},
        "columns": ["open", "high", "low", "close", "volume"],
    },
}

