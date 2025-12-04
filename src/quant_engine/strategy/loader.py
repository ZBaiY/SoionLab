# strategy/loader.py

from quant_engine.features.loader import FeatureLoader
from quant_engine.models.registry import build_model
from quant_engine.decision.loader import DecisionLoader
from quant_engine.risk.loader import RiskLoader
from quant_engine.execution.loader import ExecutionLoader
from quant_engine.portfolio.loader import PortfolioLoader


class StrategyLoader:
    @staticmethod
    def from_config(cfg, data_handler):
        """
        cfg format:
        {
            "features": {...},
            "models": {...},
            "decision": {...},
            "risk": {...},
            "execution": {...},
            "portfolio": {...}
        }
        """

        # Build feature layer
        feature_extractor = FeatureLoader.from_config(cfg["features"], data_handler)

        # Build model layer
        models = {
            name: build_model(mcfg["type"], **mcfg.get("params", {}))
            for name, mcfg in cfg["models"].items()
        }

        # Decision layer
        decision = DecisionLoader.from_config(cfg["decision"])

        # Risk layer
        risk_manager = RiskLoader.from_config(cfg["risk"])

        # Execution layer
        execution_engine = ExecutionLoader.from_config(cfg["execution"])

        # Portfolio layer
        portfolio = PortfolioLoader.from_config(cfg["portfolio"])

        # Assemble StrategyEngine
        from .engine import StrategyEngine
        return StrategyEngine(
            data_handler=data_handler,
            feature_extractor=feature_extractor,
            models=models,
            decision=decision,
            risk_manager=risk_manager,
            execution_engine=execution_engine,
            portfolio_manager=portfolio
        )