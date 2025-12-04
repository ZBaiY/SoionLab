from .registry import build_portfolio

class PortfolioLoader:
    @staticmethod
    def from_config(cfg: dict):
        name = cfg["type"]
        params = cfg.get("params", {})
        return build_portfolio(name, **params)