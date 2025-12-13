from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any

"""
{
  "timestamp": ts,
  "atm_iv": float,
  "skew": float,
  "curve": {moneyness: iv},
  "surface": {...}
}
"""

@dataclass
class IVSurfaceSnapshot:
    """
    Unified IV surface snapshot used by OptionChainDataHandler or IVSurfaceModel.

    The surface is optional; if unavailable, fallback fields remain empty.
    """

    timestamp: float
    symbol: str | None
    expiry: str | None
    model: str | None            # e.g. "SSVI", "SABR"
    atm_iv: float
    skew: float
    curve: Dict[str, float]      # moneyness → implied volatility
    surface: Dict[str, Any]      # optional SABR/SSVI parameters
    latency: float

    @classmethod
    def from_surface(
        cls,
        ts: float,
        surface_ts: float,
        atm_iv: float,
        skew: float,
        curve: Dict[str, float],
        surface_params: Dict[str, Any] | None = None,
        symbol: str | None = None,
        expiry: str | None = None,
        model: str | None = None,
    ) -> "IVSurfaceSnapshot":
        """
        Construct a snapshot from extracted surface quantities.
        """
        return cls(
            timestamp=float(surface_ts),
            symbol=symbol,
            expiry=expiry,
            model=model,
            atm_iv=float(atm_iv),
            skew=float(skew),
            curve={str(k): float(v) for k, v in curve.items()},
            surface=surface_params or {},
            latency=float(ts - surface_ts),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to plain dict for logging or JSON serialization."""
        return {
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "expiry": self.expiry,
            "model": self.model,
            "atm_iv": self.atm_iv,
            "skew": self.skew,
            "curve": self.curve,
            "surface": self.surface,
            "latency": self.latency,
        }
