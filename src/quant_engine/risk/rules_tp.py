from typing import Dict, Any
from quant_engine.contracts.risk import RiskBase
from quant_engine.risk.registry import register_risk


@register_risk("TAKE-PROFIT")
class TakeProfitRule(RiskBase):
    """
    V4 take-profit risk rule.

    Contracts:
    - no feature dependency - portfolio dependency only
    - depends on Portfolio context (PnL)
    - adjust(size, context) -> float
    """

    def __init__(self, symbol: str, **kwargs):
        max_gain = kwargs.get("max_gain", 0.1)
        super().__init__(symbol=symbol, **kwargs)
        self.max_gain = float(max_gain)

    def adjust(self, size: float, context: Dict[str, Any]) -> float:
        portfolio = context.get("portfolio", {})
        if isinstance(portfolio, dict) and "portfolio" in portfolio:
            # tolerate nested context forms ("context": {"portfolio": {...}})
            portfolio = portfolio.get("portfolio", {})

        pnl = portfolio.get("pnl")
        if pnl is None:
            realized = float(portfolio.get("realized_pnl", 0.0))
            unrealized = float(portfolio.get("unrealized_pnl", 0.0))
            pnl = realized + unrealized

        if float(pnl) > self.max_gain:
            return 0.0
        return size
