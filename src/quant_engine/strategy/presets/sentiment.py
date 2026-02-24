"""
Global sentiment presets.
"""

from __future__ import annotations

from typing import Any, Dict

SENTIMENT_PRESETS: Dict[str, Any] = {
    "sentiment": {
        "quality_mode": "TRADING",
        "model": "lexicon",
        "aggregation_window": "5m",
        "decay": None,
        "quality": {
            "policy_id": "v1",
            "reason_severity": {
                "MISSING_FRAME": {"STRICT": "HARD", "TRADING": "SOFT", "RESEARCH": "SOFT"},
                "EMPTY_FRAME": {"STRICT": "SOFT", "TRADING": "SOFT", "RESEARCH": "SOFT"},
                "STALE_DATA": {"STRICT": "SOFT", "TRADING": "SOFT", "RESEARCH": "SOFT"},
            },
        },
        "cache": {"max_bars": 10000},
    },
    "SENTIMENT_5M": {
        "$ref": "sentiment",
        "interval": "5m",
        "bootstrap": {"lookback": "30d"},
    },
    "SENTIMENT_1M": {
        "$ref": "sentiment",
        "interval": "1m",
        "bootstrap": {"lookback": "30d"},
    },
    # Backward-compatible aliases for existing presets.
    "SENTIMENT_BASIC_5M": {
        "$ref": "SENTIMENT_5M",
        "model": "lexicon",
    },
    "SENTIMENT_EMBEDDING_15M": {
        "$ref": "sentiment",
        "interval": "15m",
        "model": "embedding",
        "bootstrap": {"lookback": "30d"},
    },
}

