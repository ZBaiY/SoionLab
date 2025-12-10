from __future__ import annotations
from typing import Protocol, Optional, Any


class SentimentHandler(Protocol):
    """
    TradeBot v4 Sentiment Handler Protocol

    Enforces timestamp-aligned access for:
        • model-based sentiment (FinBERT, VADER, transformers)
        • aggregated event feeds (news, Twitter, Reddit)
        • numeric or categorical sentiment signals

    All FeatureChannels MUST obtain sentiment only through:
        • latest_score()
        • get_snapshot(ts)
        • window(ts, n)

    v4 guarantees:
        - strict anti-lookahead
        - reproducible backtest/live behaviour
        - deterministic timestamp alignment
    """

    # ---------------------------
    # Metadata
    # ---------------------------
    @property
    def symbol(self) -> str:
        """Return symbol this sentiment feed is associated with."""
        ...

    # ---------------------------
    # Required core API
    # ---------------------------
    def latest_score(self) -> float | dict[str, Any] | None:
        """
        Return the most recent sentiment signal.

        Valid return types:
            - float                → simple sentiment index
            - dict                 → richer NLP output
            - None                 → no sentiment available yet
        """
        ...

    # ---------------------------
    # v4 timestamp-aligned API
    # ---------------------------
    def get_snapshot(self, ts: float) -> Any | None:
        """
        Return the latest sentiment value whose timestamp ≤ ts.
        MUST enforce anti-lookahead.
        """
        ...

    def window(self, ts: float, n: int) -> list[Any]:
        """
        Return the most recent n sentiment values with timestamp ≤ ts.
        Used for smoothing, decay features, rolling NLP signals.
        """
        ...

    def ready(self) -> bool:
        """
        Whether at least one valid sentiment value is available.
        """
        ...

    # ---------------------------
    # Optional convenience API
    # ---------------------------
    def last_timestamp(self) -> Optional[int]:
        """
        Return timestamp (float UNIX seconds) of the most recent sentiment update.
        Used by the Engine to synchronize multi-source data.
        """
        return None

    def flush_cache(self) -> None:
        """Clear stored sentiment values (used for backtest resets)."""
        return None
