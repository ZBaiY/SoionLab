from typing import Dict, Any
from quant_engine.contracts.risk import RiskBase
from quant_engine.risk.registry import register_risk


@register_risk("STOP-LOSS")
class StopLossRule(RiskBase):
    """
    V4 stop-loss risk rule.

    Contracts:
    - no feature dependency - portfolio dependency only
    - depends on Portfolio context (PnL)
    - adjust(size, context) -> float
    """

    def __init__(self, symbol: str, **kwargs):
        max_loss = kwargs.get("max_loss", -0.05)
        super().__init__(symbol=symbol, **kwargs)
        self.max_loss = float(max_loss)

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

        if float(pnl) < self.max_loss:
            return 0.0
        return size
