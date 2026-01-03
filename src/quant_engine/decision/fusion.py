# decision/fusion.py
from quant_engine.contracts.decision import DecisionBase
from .registry import register_decision


@register_decision("FUSION")
class FusionDecision(DecisionBase):
    """Weighted fusion of multiple model outputs.

    Expected context shape (from StrategyEngine.step):
        {
            "features": {...},
            "models": {"main": <float>, ...},
            "portfolio": {...},
        }

    Params:
        weights: mapping from model-output key -> weight.
            Example: {"main": 1.0}
    """

    def __init__(self, symbol: str | None = None, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        weights = kwargs.get("weights")
        if not isinstance(weights, dict) or not weights:
            raise ValueError("FusionDecision requires non-empty weights={model_key: weight}")
        self.weights: dict[str, float] = {str(k): float(v) for k, v in weights.items()}

    def decide(self, context: dict) -> float:
        # Prefer model outputs; fallback to flat context for backward compatibility.
        models = context.get("models")
        if not isinstance(models, dict):
            models = context

        total = 0.0
        for key, w in self.weights.items():
            total += float(w) * float(models.get(key, 0.0))
        return float(total)