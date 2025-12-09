# strategy/loader.py

from quant_engine.features.loader import FeatureLoader
from quant_engine.models.registry import build_model
from quant_engine.decision.loader import DecisionLoader
from quant_engine.risk.loader import RiskLoader
from quant_engine.execution.loader import ExecutionLoader
from quant_engine.portfolio.loader import PortfolioLoader
from quant_engine.utils.logger import get_logger, log_debug
from quant_engine.strategy.symbol_discovery import discover_symbols
from quant_engine.strategy.feature_resolver import resolve_feature_config
from quant_engine.data.builder import build_multi_symbol_handlers


class StrategyLoader:
    _logger = get_logger(__name__)

    @staticmethod
    def from_config(cfg):
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

        # Primary trading symbol (root of routing)
        symbol = cfg.get("symbol")

        # ----- LAYER 2: Symbol Discovery -----
        features_user = cfg.get("features_user", [])
        model_cfg = cfg.get("model", {})
        symbols = discover_symbols(
            primary=symbol,
            features_user=features_user,
            model_cfg=model_cfg
        )
        log_debug(StrategyLoader._logger, "Symbols discovered", symbols=list(symbols))

        # ----- LAYER 3: Multi-Symbol DataHandler Initialization -----
        data_handlers = build_multi_symbol_handlers(symbols)
        log_debug(StrategyLoader._logger, "DataHandlers built", handler_types=list(data_handlers.keys()))

        log_debug(StrategyLoader._logger, "StrategyLoader received config", keys=list(cfg.keys()))

        log_debug(StrategyLoader._logger, "StrategyLoader building models")
        # Build model layer
        models = {
            "main": build_model(
                model_cfg["type"],
                symbol=symbol,
                **model_cfg.get("params", {})
            )
        }

        log_debug(StrategyLoader._logger, "StrategyLoader building risk layer")
        # Risk layer
        risk_manager = RiskLoader.from_config(cfg["risk"], symbol=symbol)

        # ----- NEW LAYER 4: Feature Dependency Resolver (after building model & risk) -----
        model_main = models["main"]
        model_required = getattr(model_main, "required_features", [])
        model_secondary = getattr(model_main, "features_secondary", [])

        risk_required = getattr(risk_manager, "required_features", [])

        final_features = resolve_feature_config(
            primary_symbol=symbol,
            user_features=features_user,
            model_required=model_required,
            model_secondary=model_secondary,
            risk_required=risk_required,
        )
        log_debug(StrategyLoader._logger, "Final feature_config generated",
                  count=len(final_features))

        # ----- NEW LAYER 5: FeatureExtractor Initialization -----
        feature_extractor = FeatureLoader.from_config(
            final_features,
            data_handlers["ohlcv"],
            data_handlers.get("orderbook", {}),
            data_handlers.get("option_chain", {}),
            data_handlers.get("sentiment", {}),
        )

        log_debug(StrategyLoader._logger, "StrategyLoader building decision layer")
        # Decision layer
        decision = DecisionLoader.from_config(cfg["decision"], symbol=symbol)

        log_debug(StrategyLoader._logger, "StrategyLoader building execution layer")
        # Execution layer
        execution_engine = ExecutionLoader.from_config(cfg["execution"], symbol=symbol)

        log_debug(StrategyLoader._logger, "StrategyLoader building portfolio layer")
        # Portfolio layer
        portfolio = PortfolioLoader.from_config(cfg["portfolio"], symbol=symbol)

        log_debug(StrategyLoader._logger, "StrategyLoader assembled StrategyEngine")
        # Assemble StrategyEngine
        from .engine import StrategyEngine
        return StrategyEngine(
            symbol=symbol,
            ohlcv_handlers=data_handlers["ohlcv"],
            orderbook_handlers=data_handlers.get("orderbook", {}),
            option_chain_handlers=data_handlers.get("option_chain", {}),
            sentiment_handlers=data_handlers.get("sentiment", {}),
            feature_extractor=feature_extractor,
            models=models,
            decision=decision,
            risk_manager=risk_manager,
            execution_engine=execution_engine,
            portfolio_manager=portfolio
        )
