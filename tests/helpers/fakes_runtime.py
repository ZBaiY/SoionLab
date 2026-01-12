from __future__ import annotations

from typing import Any

from ingestion.contracts.tick import IngestionTick
from quant_engine.contracts.model import ModelBase
from quant_engine.contracts.decision import DecisionBase
from quant_engine.contracts.risk import RiskBase
from quant_engine.contracts.portfolio import PortfolioBase, PortfolioState
from quant_engine.contracts.execution.policy import PolicyBase
from quant_engine.contracts.execution.router import RouterBase
from quant_engine.contracts.execution.slippage import SlippageBase
from quant_engine.contracts.execution.matching import MatchingBase





class DummyModel(ModelBase):
    def predict(self, features: dict[str, Any]) -> float:
        return 0.0


class DummyDecision(DecisionBase):
    def decide(self, context: dict) -> float:
        return 0.0


class DummyRisk(RiskBase):
    def adjust(self, size: float, context: dict) -> float:
        return float(size)


class DummyPolicy(PolicyBase):
    def generate(self, target_position: float, portfolio_state: dict, market_data: Any | None) -> list:
        return []


class DummyRouter(RouterBase):
    def route(self, orders: list, market_data: Any | None) -> list:
        return list(orders)


class DummySlippage(SlippageBase):
    def apply(self, orders: list, market_data: Any | None) -> list:
        return list(orders)


class DummyMatcher(MatchingBase):
    def match(self, orders: list, market_data: Any | None) -> list:
        return []


class DummyPortfolio(PortfolioBase):
    def __init__(self, symbol: str):
        super().__init__(symbol=symbol)
        self.calls: list[tuple[str, Any]] = []

    def apply_fill(self, fill: dict) -> dict | None:
        self.calls.append(("apply_fill", fill))
        return None

    def state(self) -> PortfolioState:
        return PortfolioState({"symbol": self.symbol})


class FakeWorker:
    def __init__(self, ticks: list[IngestionTick]):
        self._ticks = list(ticks)
        self.calls: list[str] = []

    async def run(self, emit) -> None:
        self.calls.append("run")
        for tick in self._ticks:
            r = emit(tick)
            if hasattr(r, "__await__"):
                await r
