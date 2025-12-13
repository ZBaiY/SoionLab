from typing import Protocol, Dict, Any


class DecisionProto(Protocol):
    """
    V4 unified decision protocol:
        • decide(context) -> float
        • context contains model_score, features, sentiment, regime, portfolio_state
        • decision module should NOT know symbol
        • decision should not filter features; model/risk handle that
    """

    def decide(self, context: Dict[str, Any]) -> float:
        ...


# ----------------------------------------------------------------------
# V4 Decision Base Class
# ----------------------------------------------------------------------
class DecisionBase(DecisionProto):
    """
    Unified base class for decision modules.

    Key properties in v4:
        • symbol-agnostic (symbol filtering belongs to ModelBase / RiskBase)
        • no required_features (decision only transforms scores)
        • must implement decide(context)
    """

    def __init__(self, **kwargs):
        # decisions do not need symbol or secondary
        pass

    def decide(self, context: Dict[str, Any]) -> float:
        raise NotImplementedError("Decision module must implement decide()")