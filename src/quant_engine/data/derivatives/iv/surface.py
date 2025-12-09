# quant_engine/iv/surface.py

from abc import ABC, abstractmethod
from quant_engine.data.derivatives.option_chain import OptionChain


class IVSurfaceModel(ABC):
    """
    Abstract interface for IV surface models (SABR, SSVI, etc.)

    Every concrete model must:
        - fit(OptionChain) -> self
        - atm_iv()
        - smile_slope()
        - smile_curvature()
    """

    @abstractmethod
    def fit(self, chain: OptionChain):
        """Fit model parameters to the option chain."""
        raise NotImplementedError

    @abstractmethod
    def atm_iv(self) -> float:
        raise NotImplementedError

    @abstractmethod
    def smile_slope(self) -> float:
        raise NotImplementedError

    @abstractmethod
    def smile_curvature(self) -> float:
        raise NotImplementedError