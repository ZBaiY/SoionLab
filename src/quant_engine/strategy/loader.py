# strategy/loader.py
from typing import cast, Any
from collections.abc import Mapping
from quant_engine.contracts.model import ModelBase
from quant_engine.strategy.base import StrategyBase
from quant_engine.strategy.config import NormalizedStrategyCfg
from quant_engine.features.loader import FeatureLoader
from quant_engine.models.registry import build_model
from quant_engine.decision.loader import DecisionLoader
from quant_engine.risk.loader import RiskLoader
from quant_engine.execution.loader import ExecutionLoader
from quant_engine.portfolio.loader import PortfolioLoader
from quant_engine.strategy.feature_resolver import resolve_feature_config, check_missing_features
from quant_engine.data.builder import build_multi_symbol_handlers
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.data.contracts.protocol_realtime import OHLCVHandlerProto, RealTimeDataHandler, to_interval_ms
from quant_engine.utils.logger import get_logger, log_info, compute_config_hash

class StrategyLoader:

    @staticmethod
    def from_config(
        strategy: StrategyBase | type[StrategyBase] | NormalizedStrategyCfg | Mapping[str, Any],
        mode: EngineMode,
        overrides: dict | None = None,
        symbols: dict[str, str] | None = None,
    ):
        """
        Build a StrategyEngine from a StrategyBase specification and mode.
        """

        if overrides is None:
            overrides = {}

        strategy_cls: type[StrategyBase] | None = None
        strategy_name = None

        if isinstance(strategy, NormalizedStrategyCfg):
            if overrides:
                raise ValueError("overrides must be empty when passing a normalized strategy cfg")
            cfg = strategy.to_dict()
            strategy_name = strategy.strategy_name
        elif isinstance(strategy, StrategyBase):
            strategy_cls = strategy.__class__
            bound_symbols = getattr(strategy, "_bound_symbols", None)
            use_symbols = symbols if symbols is not None else bound_symbols
            cfg_obj = strategy_cls.standardize(overrides, symbols=use_symbols)
            cfg = cfg_obj.to_dict() if isinstance(cfg_obj, NormalizedStrategyCfg) else cfg_obj
            strategy_name = strategy_cls.STRATEGY_NAME
        elif isinstance(strategy, type) and issubclass(strategy, StrategyBase):
            strategy_cls = strategy
            cfg_obj = strategy.standardize(overrides, symbols=symbols)
            cfg = cfg_obj.to_dict() if isinstance(cfg_obj, NormalizedStrategyCfg) else cfg_obj
            strategy_name = strategy_cls.STRATEGY_NAME
        elif isinstance(strategy, Mapping):
            if overrides:
                raise ValueError("overrides must be empty when passing a raw cfg mapping")
            cfg = dict(strategy)
            strategy_block = cfg.get("strategy")
            if isinstance(strategy_block, dict):
                name = strategy_block.get("name")
                if isinstance(name, str) and name:
                    strategy_name = name
        else:
            raise TypeError("strategy must be a StrategyBase, Strategy subclass, or normalized cfg")

        # to cast symbol mapping and etc.
        universe = cfg.get("universe") or {}
        if not isinstance(universe, dict) or not universe:
            raise ValueError(
                "Strategy must be bound via StrategyCls.standardize(..., symbols=...) or strategy.bind(...) before loading"
            )

        symbol = cfg.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("Primary symbol must be resolved from bound strategy universe")

        interval = cfg.get("interval")
        if not isinstance(interval, str) or not interval:
            raise ValueError(
                "Strategy observation interval must be resolved from standardized config"
            )
        interval_ms = cfg.get("interval_ms")
        if interval_ms is None:
            ms = to_interval_ms(interval)
            if ms is None:
                raise ValueError(f"Invalid interval format: {interval}")
            interval_ms = int(ms)
        else:
            try:
                interval_ms = int(interval_ms)
            except Exception:
                raise ValueError(f"interval_ms must be an int (epoch ms), got: {interval_ms!r}")

        features_user = cfg.get("features_user", [])

        # ---------------------------------
        # 1st Validating the config
        # ---------------------------------

        # Validate feature name uniqueness (no silent dedup)
        declared_names_list = [f.get("name") for f in features_user if isinstance(f, dict) and f.get("name")]
        declared_names_set = set(declared_names_list)
        if len(declared_names_list) != len(declared_names_set):
            dupes: list[str] = []
            seen: set[str] = set()
            for n in declared_names_list:
                if n in seen and n not in dupes:
                    dupes.append(n)
                assert isinstance(n, str)
                seen.add(n)
            raise ValueError(f"Duplicate feature names in features_user: {dupes}")

        model_cfg = cfg.get("model")
        if model_cfg is not None and not isinstance(model_cfg, dict):
            raise TypeError("model must be a dict or None")
        model_params: dict = {}
        if isinstance(model_cfg, dict):
            params = model_cfg.get("params") or {}
            if not isinstance(params, dict):
                raise TypeError("model.params must be a dict")
            model_params = params

        data_spec = cfg.get("data") or {}
        secondary_spec = data_spec.get("secondary") or {}
        declared_symbols = {symbol} | set(secondary_spec.keys())

        referenced_symbols: set[str] = set()
        for f in features_user:
            if not isinstance(f, dict):
                continue
            sym = f.get("symbol")
            if isinstance(sym, str) and sym:
                referenced_symbols.add(sym)
            params = f.get("params") or {}
            if isinstance(params, dict):
                ref = params.get("ref")
                if isinstance(ref, str) and ref:
                    referenced_symbols.add(ref)

        if model_params:
            for k in ("secondary", "ref"):
                v = model_params.get(k)
                if isinstance(v, str) and v:
                    referenced_symbols.add(v)

        missing_syms = sorted(referenced_symbols - declared_symbols)
        if missing_syms:
            raise ValueError(
                f"Symbols {missing_syms} are referenced by features/model but not declared in Strategy.DATA.secondary. "
                f"Declared symbols: {sorted(declared_symbols)}"
            )
    
        # ---------------------------------
        # 1st massive Validation complete
        # ---------------------------------

        data_handlers = build_multi_symbol_handlers(
            data_spec=data_spec,
            mode=mode,
            universe=universe,
            primary_symbol=symbol,
        )

        required_data = set(cfg.get("required_data") or (strategy_cls.REQUIRED_DATA if strategy_cls else []))
        if "ohlcv" in required_data and "ohlcv" not in data_handlers:
            strategy_label = strategy_name or str(strategy)
            raise RuntimeError(
                f"Strategy '{strategy_label}' declares OHLCV but no OHLCV handler was provisioned"
            )


        models: dict[str, ModelBase] = {}
        model_main = None
        model_required: set[str] = set()
        if model_cfg:
            model_type = model_cfg.get("type")
            if not isinstance(model_type, str) or not model_type:
                raise ValueError("model.type must be a non-empty string")
            models = {
                "main": build_model(
                    model_type,
                    symbol=symbol,
                    **model_params,
                )
            }
            model_main = models["main"]
            model_required = getattr(model_main, "required_feature_types", set())
        portfolio = PortfolioLoader.from_config(symbol=symbol, cfg=cfg["portfolio"])
        portfolio_state = portfolio.state().to_dict()
        log_info(
            get_logger(__name__),
            "loader.portfolio.init",
            symbol=symbol,
            portfolio_type=cfg.get("portfolio", {}).get("type"),
            qty_step=portfolio_state.get("qty_step"),
            qty_mode=portfolio_state.get("qty_mode"),
        )
        risk_manager = RiskLoader.from_config(symbol=symbol, cfg=cfg["risk"])
        decision = DecisionLoader.from_config(symbol=symbol, cfg=cfg["decision"])
        if getattr(decision, "symbol", None) is None:
            try:
                decision.symbol = symbol
            except Exception:
                pass


        # ----- NEW LAYER 4: Feature Dependency Resolver (after building model & risk) -----
        risk_required: set[str] = set()
        for rule in getattr(risk_manager, "rules", []):
            risk_required |= getattr(rule, "required_feature_types", set())

        # -------------------------------------------------
        # 2ed Validating the config -- feature dependencies
        # -------------------------------------------------

        final_features = resolve_feature_config(
            primary_symbol=symbol,
            user_features=features_user,
            required_feature_types=model_required | risk_required,
        )
        # resolved_feature_names = {f["name"] for f in final_features}

        resolved_feature_names = {f["name"] for f in final_features if isinstance(f, dict) and f.get("name")}
        model_feature_names = {n for n in resolved_feature_names if "_MODEL_" in n}
        risk_feature_names = {n for n in resolved_feature_names if "_RISK_" in n}
        decision_feature_names = {n for n in resolved_feature_names if "_DECISION_" in n}

        if model_main is not None and hasattr(model_main, "set_required_features"):
            model_main.set_required_features(model_feature_names)

        for rule in getattr(risk_manager, "rules", []):
            if hasattr(rule, "set_required_features"):
                rule.set_required_features(risk_feature_names)

        if hasattr(decision, "set_required_features"):
            decision.set_required_features(decision_feature_names)
        # Bind semantic lookup index for fname()/fget() convenience (validation sets remain separate)
        if model_main is not None and hasattr(model_main, "bind_feature_index"):
            model_main.bind_feature_index(resolved_feature_names)
        for rule in getattr(risk_manager, "rules", []):
            if hasattr(rule, "bind_feature_index"):
                rule.bind_feature_index(resolved_feature_names)
        if hasattr(decision, "bind_feature_index"):
            decision.bind_feature_index(resolved_feature_names)

        # ----- LAYER 5: Post-build feature validation & binding -----
        check_missing_features(
            feature_configs=final_features,
            model=model_main,
            risk_manager=risk_manager,
            decision=decision,
        )

        # -----------------------------------------------
        # 2ed Validation finished -- feature dependencies
        # -----------------------------------------------
        execution_engine = ExecutionLoader.from_config(symbol=symbol, cfg=cfg["execution"])

        feature_extractor = FeatureLoader.from_config(
            final_features,
            data_handlers.get("ohlcv", {}),
            data_handlers.get("orderbook", {}),
            data_handlers.get("option_chain", {}),
            data_handlers.get("iv_surface", {}),
            data_handlers.get("sentiment", {}),
            data_handlers.get("trades", {}),
            data_handlers.get("option_trades", {}),
        )
        # Inject strategy observation interval early (authoritative)
        if hasattr(feature_extractor, "set_interval"):
            feature_extractor.set_interval(interval)
        if hasattr(feature_extractor, "set_interval_ms"):
            try:
                feature_extractor.set_interval_ms(int(interval_ms))
            except Exception:
                pass

        # -----------------------
        # Assemble StrategyEngine
        # -----------------------

        ohlcv_handlers = cast(
            Mapping[str, OHLCVHandlerProto],
            data_handlers.get("ohlcv", {})
        )
        orderbook_handlers = cast(
            Mapping[str, RealTimeDataHandler],
            data_handlers.get("orderbook", {})
        )
        option_chain_handlers = cast(
            Mapping[str, RealTimeDataHandler],
            data_handlers.get("option_chain", {})
        )
        iv_surface_handlers = cast(
            Mapping[str, RealTimeDataHandler],
            data_handlers.get("iv_surface", {})
        )
        sentiment_handlers = cast(
            Mapping[str, RealTimeDataHandler],
            data_handlers.get("sentiment", {})
        )
        trades_handlers = cast(
            Mapping[str, RealTimeDataHandler],
            data_handlers.get("trades", {})
        )
        option_trades_handlers = cast(
            Mapping[str, RealTimeDataHandler],
            data_handlers.get("option_trades", {})
        )

        spec = EngineSpec(
            mode=mode,
            symbol=symbol,
            interval=interval,
            interval_ms=int(interval_ms),
            universe=universe,
        )

        engine = StrategyEngine(
            spec=spec,
            ohlcv_handlers=ohlcv_handlers,
            orderbook_handlers=orderbook_handlers,
            option_chain_handlers=option_chain_handlers,
            iv_surface_handlers=iv_surface_handlers,
            sentiment_handlers=sentiment_handlers,
            trades_handlers=trades_handlers,
            option_trades_handlers=option_trades_handlers,
            feature_extractor=feature_extractor,
            models=models,
            decision=decision,
            risk_manager=risk_manager,
            execution_engine=execution_engine,
            portfolio_manager=portfolio
        )
        config_hash = compute_config_hash(cfg)
        engine.strategy_name = strategy_name or "<unnamed>"
        engine.config_hash = config_hash
        return engine
