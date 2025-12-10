from __future__ import annotations
from typing import Protocol, Optional, Any
from quant_engine.data.derivatives.iv.surface import IVSurface


class IVSurfaceHandler(Protocol):
    """
    Protocol for IV Surface managers in TradeBot v4.

    This handler abstracts away the details of:
        - constructing IV surfaces (SSVI / SABR / spline / MC)
        - caching surfaces across ticks
        - serving features / models with stable IV queries

    All modes (backtest / mock / live) must satisfy this unified API.
    """

    # ---------------------------
    # Metadata
    # ---------------------------
    @property
    def symbol(self) -> str:
        """Return symbol whose option surface this handler manages."""
        ...

    # ---------------------------
    # Required core API
    # ---------------------------
    def latest_surface(self) -> Optional[IVSurface]:
        """
        Return the most recently built IV surface.
        Returns None if no surface is available yet.
        """
        ...

    def build_surface(self) -> Optional[IVSurface]:
        """
        Recompute (or lazily update) the IV surface using the latest option chain.
        Should internally:
            - fetch latest option chain
            - calibrate via SSVI/SABR/etc
            - cache the constructed IVSurface object
        """
        ...

    def ready(self) -> bool:
        """
        Whether the handler has sufficient data (option chain snapshots)
        to construct a reliable IV surface.
        """
        ...

    # ---------------------------
    # Optional convenience API
    # ---------------------------
    def flush_cache(self) -> None:
        """Clear cached surfaces (used for backtest resets)."""
        return None

    def last_timestamp(self) -> Optional[int]:
        """Timestamp of the latest surface if available."""
        return None
