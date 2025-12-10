from __future__ import annotations
from typing import Protocol, Any, Optional
from quant_engine.data.derivatives.option_chain.option_chain import OptionChain

""" 
{
    "timestamp": ...,
    "options": [
         {"strike":..., "expiry":..., "type":..., "iv":..., "delta":...},
         ...
    ],
    "underlying_price": ...
}
"""

class OptionChainHandler(Protocol):
    """
    TradeBot v4 Option Chain Handler Protocol

    Enforces a unified, timestamp-aligned API across:
        • historical backtests
        • mock streaming
        • real-time live handlers

    All FeatureChannels **must** obtain option data through:
        • latest_chain()
        • get_snapshot(ts)
        • window(ts, n)

    v4 guarantees:
        - strict anti-lookahead
        - deterministic chain selection
        - reproducible backtest/live behavior
    """

    # ---------------------------
    # Metadata
    # ---------------------------
    @property
    def symbol(self) -> str:
        """Return underlying symbol this chain handler belongs to."""
        ...

    # ---------------------------
    # Required core API
    # ---------------------------
    def latest_chain(self) -> OptionChain | None:
        """
        Return the most recent option chain snapshot for the symbol.
        Must return:
            - OptionChain object, or
            - None if unavailable yet
        """
        ...

    def get_chain(self, expiry: str | None) -> OptionChain | None:
        """
        Return the option chain for the requested expiry.
        expiry may be:
            - "next" / "front" → nearest expiry
            - explicit expiry string (e.g., "2025-03-28")
        """
        ...


    # ---------------------------
    # v4 timestamp-aligned API
    # ---------------------------
    def get_snapshot(self, ts: float) -> OptionChain | None:
        """
        Return the latest OptionChain whose timestamp ≤ ts.
        MUST enforce anti-lookahead and deterministic ordering.
        """
        ...

    def window(self, ts: float, n: int) -> list[OptionChain]:
        """
        Return the most recent n OptionChain objects with timestamp ≤ ts.
        Used for:
            • IV drift features
            • skew/smile momentum
            • expiry‑aware rolling models
        """
        ...

    def ready(self) -> bool:
        """
        Whether the handler contains at least one valid chain snapshot.
        """
        ...

    # ---------------------------
    # Optional convenience API
    # ---------------------------
    def last_timestamp(self) -> Optional[int]:
        """
        Timestamp (float UNIX seconds) of the most recent option chain.
        Returns None if no chain is available yet.
        """
        return None

    def flush_cache(self) -> None:
        """Clear cached chains (used for backtest resets)."""
        return None
