from quant_engine.contracts.portfolio import PortfolioManagerProto

PORTFOLIO_REGISTRY: dict[str, type[PortfolioManagerProto]] = {}

def register_portfolio(name: str):
    def decorator(cls):
        PORTFOLIO_REGISTRY[name] = cls
        return cls
    return decorator


def build_portfolio(name: str, **kwargs) -> PortfolioManagerProto:
    if name not in PORTFOLIO_REGISTRY:
        raise ValueError(f"Portfolio '{name}' not found.")
    return PORTFOLIO_REGISTRY[name](**kwargs)