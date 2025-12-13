# contracts/portfolio.py
from typing import Protocol, Dict
from dataclasses import dataclass

"""
┌──────────────────────────┐
│   MatchingEngine / Live  │
└──────────────┬───────────┘
               │  apply_fill()
               ▼
┌──────────────────────────┐
│   PortfolioManagerProto  │  <─── Contract
└──────────────┬───────────┘
    updates    │ returns
positions, PnL │ PortfolioState
               ▼
┌──────────────────────────┐
│      PortfolioState      │  <─── Read-only snapshot
└──────────────────────────┘
                ▲
    used by Strategy / Risk / Reporter
"""

@dataclass
class PositionRecord:
    symbol: str
    qty: float
    entry_price: float
    unrealized_pnl: float = 0.0


@dataclass
class PortfolioState:
    snapshot_dict: Dict

    def to_dict(self) -> Dict:
        return self.snapshot_dict


class PortfolioManagerProto(Protocol):
    """
    Core accounting interface.
    Receives fills from MatchingEngine.
    Updates positions, PnL, metrics.
    """
    symbol: str

    def apply_fill(self, fill: Dict):
        """Update portfolio based on fill dict."""
        ...

    def state(self) -> PortfolioState:
        """Return current state."""
        ...


class PortfolioBase(PortfolioManagerProto):
    """
    V4 unified portfolio base:
        • symbol-aware (primary trading symbol)
        • child classes store positions, cash, pnl
        • must implement apply_fill() and state()
    """

    def __init__(self, symbol: str, **kwargs):
        self.symbol = symbol

    def apply_fill(self, fill: Dict):
        raise NotImplementedError("Portfolio must implement apply_fill()")

    def state(self) -> PortfolioState:
        raise NotImplementedError("Portfolio must implement state()")