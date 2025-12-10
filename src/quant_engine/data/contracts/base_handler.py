from __future__ import annotations
from typing import Protocol, Any, Dict, Optional
import pandas as pd


class BaseDataHandler(Protocol):
    """
    Base interface for all data handlers in TradeBot v4.

    Requirements (v4 unified contract):
        • symbol                 → instrument identifier
        • latest()               → latest raw unit (bar / snapshot / chain / sentiment)
        • latest_timestamp()     → timestamp of latest unit
        • get_snapshot(ts)       → timestamp‑aligned, anti‑lookahead snapshot
        • window(ts, n)          → timestamp‑aligned rolling window (OHLCV, orderbook, options)
        • ready()                → whether handler is ready for use

    Notes:
        - Historical / Realtime / Mock handlers must all implement these.
        - `get_snapshot(ts)` is the ONLY valid way for features/models to access aligned data.
        - `window(ts, n)` MUST return only items whose timestamp ≤ ts.
    """

    # ---------------------------
    # Metadata
    # ---------------------------
    @property
    def symbol(self) -> str:
        """Return symbol or instrument name."""
        ...

    # ---------------------------
    # Core API (required)
    # ---------------------------
    def latest(self) -> Any:
        """
        Return the latest "unit" of data.
        
        For OHLCV → latest bar (DataFrame or dict)
        For Orderbook → latest snapshot
        For OptionChain → latest chain structure
        For Sentiment → latest sentiment score
        """
        ...

    # ---------------------------
    # v4 timestamp‑aligned API
    # ---------------------------
    def get_snapshot(self, ts: float) -> Any:
        """
        Return the latest aligned snapshot such that snapshot.timestamp ≤ ts.
        Must be anti‑lookahead and deterministic.
        """
        ...

    def window(self, ts: float, n: int):
        """
        Return the last n items whose timestamp ≤ ts.
        Must be anti‑lookahead.

        For OHLCV     → DataFrame of bars
        For Orderbook → List[OrderbookSnapshot]
        For Options   → List[OptionChain]
        """
        ...

    def ready(self) -> bool:
        """
        Whether the handler has enough internal data to produce valid outputs.

        Example:
            RealTimeOHLCVHandler → True if at least one bar exists.
            HistoricalHandler    → True once data loaded successfully.
        """
        ...

    # ---------------------------
    # Optional convenience API
    # ---------------------------
    def latest_timestamp(self) -> Optional[int]:
        """
        Return timestamp of the latest item.
        Used by the Engine to determine when a handler has progressed.

        Must return float (UNIX seconds) or None.
        """
        return None

    def flush_cache(self) -> None:
        """Clear internal buffers (useful for backtest resets)."""
        return None