from quant_engine.contracts.portfolio import PortfolioBase

PORTFOLIO_REGISTRY: dict[str, type[PortfolioBase]] = {}

def register_portfolio(name: str):
    def decorator(cls):
        PORTFOLIO_REGISTRY[name] = cls
        return cls
    return decorator


def build_portfolio(name: str, symbol: str, **kwargs) -> PortfolioBase:
    if name not in PORTFOLIO_REGISTRY:
        raise ValueError(f"Portfolio '{name}' not found.")
    return PORTFOLIO_REGISTRY[name](symbol=symbol, **kwargs)

from .manager import *
from .fractional import *
