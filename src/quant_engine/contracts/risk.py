from typing import Protocol, Dict, Any, List


class RiskProto(Protocol):
    """
    V4 unified risk protocol:
        • adjust(size, features) -> float
        • symbol-aware (primary symbol)
        • may declare required_features (ATR, VOL, etc.)
    """

    symbol: str
    required_features: List[str]

    def adjust(self, size: float, features: Dict[str, Any]) -> float:
        ...


# ----------------------------------------------------------------------
# V4 Risk Base Class
# ----------------------------------------------------------------------
class RiskBase(RiskProto):
    """
    Unified base class for all risk modules in the engine.

    Responsibilities:
        • store primary symbol (self.symbol)
        • declare required_features (ATR, VOL, etc.)
        • provide feature filtering helpers (symbol-level)
        • child class must implement adjust()
    """

    required_features: List[str] = []   # e.g. ["ATR"], ["VOL"], ["ATR", "VOL"]
    symbol: str | None = None

    def __init__(self, symbol: str, **kwargs):
        self.symbol = symbol
        self.required_features = [r + f"_{symbol}" for r in self.required_features]

    # ------------------------------------------------------------------
    # Feature filtering — same logic as ModelBase.filter_symbol
    # ------------------------------------------------------------------
    def filter_symbol(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Keep features for primary symbol only."""
        suffix = f"_{self.symbol}"
        return {k: v for k, v in features.items() if k.endswith(suffix)}

    # ------------------------------------------------------------------
    # Child classes must implement adjust()
    # ------------------------------------------------------------------
    def adjust(self, size: float, features: Dict[str, Any]) -> float:
        raise NotImplementedError("Risk module must implement adjust()")