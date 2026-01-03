# decision/regime.py
from quant_engine.contracts.decision import DecisionBase
from .registry import register_decision


@register_decision("REGIME")
class RegimeDecision(DecisionBase):
    """Map a regime label to a decision score.

    Expected context shape (from StrategyEngine.step):
        {
            "features": {...},
            "models": {"main": <float>, ...} OR {"regime_label": <float>, ...},
            "portfolio": {...},
        }

    Backward compatible with legacy flat contexts that put `regime_label` at top level.
    """

    def __init__(
        self,
        symbol: str | None = None,
        **kwargs,
    ):
        super().__init__(symbol=symbol, **kwargs)
        self.bull = float(kwargs.get("bull", 1.0))
        self.bear = float(kwargs.get("bear", -1.0))

    def decide(self, context: dict) -> float:
        # Prefer model outputs; tolerate multiple key conventions.
        models = context.get("models")
        if not isinstance(models, dict):
            models = {}

        regime_label = models.get("regime_label")
        if regime_label is None:
            regime_label = models.get("main")
        if regime_label is None:
            # Legacy/flat context fallback
            regime_label = context.get("regime_label", 0.0)

        x = float(regime_label)
        if x > 0.0:
            return self.bull
        if x < 0.0:
            return self.bear
        return 0.0