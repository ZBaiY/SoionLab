from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Tick:
    """
    Runtime ingestion envelope.

    """

    domain: str        # e.g. "ohlcv", "orderbook", "sentiment", "iv_surface"
    payload: Any       # domain-specific payload or derived trigger
    event_ts: float    # event-time (source / observation time)
