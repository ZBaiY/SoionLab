from __future__ import annotations
from typing import Protocol, Any, Dict, Optional
import pandas as pd


class OrderbookHandler(Protocol):
    """
    TradeBot v4 Orderbook Handler Protocol

    Enforces unified, timestamp-aligned access across:
        • Historical backtests
        • Mock streaming
        • Live real-time feeds

    All FeatureChannels MUST obtain orderbook data only through:
        • latest_snapshot()
        • get_snapshot(ts)
        • window(ts, n)

    v4 guarantees:
        - strict anti-lookahead
        - deterministic reproducibility
        - unified behavior across all execution modes
    """

    # ---------------------------
    # Metadata
    # ---------------------------
    @property
    def symbol(self) -> str:
        """Return symbol associated with this handler."""
        ...

    # ---------------------------
    # Required core API
    # ---------------------------
    def latest_snapshot(self) -> Dict[str, Any]:
        """
        Return the most recent orderbook snapshot.

        Expected structure (example):
            {
                "bid_price": float,
                "ask_price": float,
                "bid_size": float,
                "ask_size": float,
                "bids": [(price, size), ...],   # optional
                "asks": [(price, size), ...]
            }
        """
        ...

    # ---------------------------
    # v4 timestamp-aligned API
    # ---------------------------
    def get_snapshot(self, ts: float) -> Dict[str, Any] | None:
        """
        Return the latest orderbook snapshot whose timestamp ≤ ts.
        MUST enforce anti-lookahead.
        """
        ...

    def window(self, ts: float, n: int) -> list[Dict[str, Any]]:
        """
        Return the most recent n snapshots with timestamp ≤ ts.
        Used for rolling microstructure features (spread, imbalance, depth).
        """
        ...

    def ready(self) -> bool:
        """
        Whether the handler has accumulated at least one valid snapshot.
        """
        ...

    # ---------------------------
    # Optional convenience API
    # ---------------------------
    def last_timestamp(self) -> Optional[int]:
        """
        Timestamp (float UNIX seconds) of the most recent snapshot.
        Engine uses this to synchronize multi-source data.
        """
        return None

    def flush_cache(self) -> None:
        """Clear internal stored data (used by backtest resets)."""
        return None
