# quant_engine/features/iv_surface.py

from quant_engine.contracts.feature import FeatureChannel
from quant_engine.data.derivatives.option_chain import OptionChain
from quant_engine.iv.surface import IVSurfaceModel  # future SABR/SSVI model
from quant_engine.iv.ssvi import SSVIModel          # optional
from quant_engine.iv.sabr import SABRModel          # optional

from quant_engine.utils.logger import get_logger, log_debug, log_info

from quant_engine.features.registry import register_feature
from quant_engine.data.derivatives.chain_handler import OptionChainDataHandler

"""
IVSurfaceFeature
----------------------------------------
FeatureChannel that consumes OptionChain and produces
surface-based features such as:
    - atm_iv
    - skew_25d
    - curvature
    - ssvi_eta
    - sabr_alpha

This feature does NOT do SABR/SSVI implementation itself.
It only delegates to iv/surface.py which does the heavy math.
"""

@register_feature("IV_SURFACE")
class IVSurfaceFeature(FeatureChannel):
    _logger = get_logger(__name__)
    def __init__(
        self,
        symbol=None,
        **kwargs
    ):
        
        self._symbol = symbol
        self._atm_iv = None
        self._skew = None
        self._curvature = None
        self.expiry = kwargs.get("expiry", "next")
        self.model = kwargs.get("model", "SSVI")
        self.model_kwargs = kwargs.get("model_kwargs", {}) or {}
        log_debug(
            self._logger,
            "IVSurfaceFeature initialized",
            symbol=self._symbol,
            expiry=self.expiry,
            model=self.model,
            model_kwargs=self.model_kwargs,
        )

    @property
    def symbol(self):
        return self._symbol

    def _fit_surface(self, chain: OptionChain):
        """
        Delegates surface fitting to modeling layer.
        """
        log_debug(self._logger, "IVSurfaceFeature fitting surface", model=self.model)
        match self.model.upper():
            case "SSVI":
                model = SSVIModel(**self.model_kwargs)
                return model.fit(chain)

            case "SABR":
                model = SABRModel(**self.model_kwargs)
                return model.fit(chain)

            case _:
                raise ValueError(f"Unsupported IV model: {self.model}")

    def initialize(self, context):
        """
        Full-window initialization: fit surface once using the latest option chain.
        """
        chain_handler = context.get("option_chain")
        if chain_handler is None:
            self._atm_iv = None
            self._skew = None
            self._curvature = None
            return

        chain = chain_handler.get_chain(self.expiry)
        if chain is None:
            self._atm_iv = None
            self._skew = None
            self._curvature = None
            return

        surface = self._fit_surface(chain)
        self._atm_iv = float(surface.atm_iv())
        self._skew = float(surface.smile_slope())
        self._curvature = float(surface.smile_curvature())

    def update(self, context):
        """
        Incremental update: refit surface only using the most recent option chain snapshot.
        """
        chain_handler = context.get("option_chain")
        if chain_handler is None:
            return

        chain = chain_handler.get_chain(self.expiry)
        if chain is None:
            return

        surface = self._fit_surface(chain)
        self._atm_iv = float(surface.atm_iv())
        self._skew = float(surface.smile_slope())
        self._curvature = float(surface.smile_curvature())

    def output(self):
        """Return the latest surface-based features."""
        return {
            "ivsurf_atm": self._atm_iv,
            "ivsurf_skew": self._skew,
            "ivsurf_curvature": self._curvature,
        }