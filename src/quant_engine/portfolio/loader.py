from quant_engine.portfolio.registry import build_portfolio
from quant_engine.utils.logger import get_logger, log_debug
"""
Stat Arb -- e.g. OU equation ... We adapt long A, short B correspondingly at here, it will not go through decisions/models.
"""


class PortfolioLoader:
    _logger = get_logger(__name__)
    @staticmethod
    def from_config(symbol: str, cfg: dict):
        log_debug(PortfolioLoader._logger, "PortfolioLoader received config", config=cfg)
        name = cfg["type"]
        assert isinstance(name, str) and name, "Portfolio type must be a non-empty string"
        params = cfg.get("params", {})
        log_debug(PortfolioLoader._logger, "PortfolioLoader built portfolio", name=name, params=params)
        return build_portfolio(name, symbol=symbol, **params)