# contracts/portfolio.py
from typing import Protocol, Dict
from dataclasses import dataclass


@dataclass
class PositionRecord:
    qty: float
    entry_price: float
    unrealized_pnl: float = 0.0


class PortfolioState(Protocol):
    """
    Queried by StrategyEngine / Risk / Reporting.
    Should contain:
    - positions
    - account equity
    - exposure
    - leverage
    """
    def snapshot(self) -> Dict:
        ...


class PortfolioManagerProto(Protocol):
    """
    Core accounting interface.
    Receives fills from MatchingEngine.
    Updates positions, PnL, metrics.
    """
    def apply_fill(self, fill: Dict):
        """Update portfolio based on fill dict."""

    def state(self) -> PortfolioState:
        """Return current state."""