from abc import ABC, abstractmethod
from typing import Any, Iterable
from quant_engine.data.derivatives.option_chain.option_chain import OptionChain


class IVSurface(ABC):
    """
    Abstract interface for implied volatility surfaces (SABR, SSVI, spline, MC, etc.)

    A valid surface must support:
        - Fitting from an OptionChain
        - Querying σ(K, T)
        - ATM IV / slope / curvature summary stats
        - Expiry / strike grids for interpolation
        - Timestamp tracking
    """

    # --------------------------------------------------------
    # Core fitting API
    # --------------------------------------------------------
    @abstractmethod
    def fit(self, chain: OptionChain):
        """Fit the IV surface to the given option chain."""
        raise NotImplementedError

    # --------------------------------------------------------
    # Core query API (must-have)
    # --------------------------------------------------------
    @abstractmethod
    def sigma(self, strike: float, expiry: str) -> float:
        """
        Return IV σ(K, T) for the given strike and expiry.
        Must be implemented by all surface models.
        """
        raise NotImplementedError

    # --------------------------------------------------------
    # Summary metrics
    # --------------------------------------------------------
    @abstractmethod
    def atm_iv(self) -> float:
        raise NotImplementedError

    @abstractmethod
    def smile_slope(self) -> float:
        raise NotImplementedError

    @abstractmethod
    def smile_curvature(self) -> float:
        raise NotImplementedError

    # --------------------------------------------------------
    # Grid helpers (optional but recommended)
    # --------------------------------------------------------
    def expiries(self) -> Iterable[str]:
        """Return available expiries used by this surface."""
        raise NotImplementedError

    def strikes(self) -> Iterable[float]:
        """Return available strikes used by this surface."""
        raise NotImplementedError

    # --------------------------------------------------------
    # Metadata
    # --------------------------------------------------------
    def last_timestamp(self) -> int | None:
        """Timestamp of the option chain used to build this surface."""
        return None

    # --------------------------------------------------------
    # Utility
    # --------------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary summarizing the surface (for logging/debugging)."""
        return {
            "atm_iv": self.atm_iv(),
            "slope": self.smile_slope(),
            "curvature": self.smile_curvature(),
        }