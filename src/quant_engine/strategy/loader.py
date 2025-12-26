# strategy/loader.py
from typing import cast
from collections.abc import Mapping
from quant_engine.strategy.base import StrategyBase
from quant_engine.data.derivatives.iv.iv_handler import IVSurfaceDataHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.data.orderbook.realtime import RealTimeOrderbookHandler
from quant_engine.data.sentiment.sentiment_handler import SentimentDataHandler
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
from quant_engine.data.contracts.protocol_realtime import to_interval_ms

class StrategyLoader:

    @staticmethod
    def from_config(strategy: StrategyBase, mode: EngineMode, overrides: dict | None = None):
        """
        Build a StrategyEngine from a StrategyBase specification and mode.
        """

        if overrides is None:
            overrides = {}

        # StrategyBase is the source of truth: normalize semi-JSON spec (presets/$ref, interval, ref, naming)
        if hasattr(strategy, "standardize"):
            cfg = strategy.standardize(overrides)
        else:
            cfg = strategy.apply_defaults(overrides)

        # to cast symbol mapping and etc.
        universe = cfg.get("universe") or {}
        if not isinstance(universe, dict) or not universe:
            raise ValueError("Strategy must be bound via strategy.bind(...) before loading")

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

        model_cfg = cfg.get("model", {})

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

        params = model_cfg.get("params") or {}
        if isinstance(params, dict):
            for k in ("secondary", "ref"):
                v = params.get(k)
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

        required_data = set(cfg.get("required_data") or strategy.REQUIRED_DATA)

        if "ohlcv" in required_data and "ohlcv" not in data_handlers:
            raise RuntimeError(
                f"Strategy '{strategy}' declares OHLCV but no OHLCV handler was provisioned"
            )


        model_params = model_cfg.get("params") or {}
        if not isinstance(model_params, dict):
            raise TypeError("model.params must be a dict")
        models = {
            "main": build_model(
                model_cfg["type"],
                symbol=symbol,
                **model_params,
            )
        }
        risk_manager = RiskLoader.from_config(cfg["risk"], symbol=symbol)
        decision = DecisionLoader.from_config(cfg["decision"], symbol=symbol)
        if getattr(decision, "symbol", None) is None:
            try:
                decision.symbol = symbol
            except Exception:
                pass


        # ----- NEW LAYER 4: Feature Dependency Resolver (after building model & risk) -----
        model_main = models["main"]
        model_required = getattr(model_main, "required_feature_types", set())
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

        if hasattr(model_main, "set_required_features"):
            model_main.set_required_features(model_feature_names)

        for rule in getattr(risk_manager, "rules", []):
            if hasattr(rule, "set_required_features"):
                rule.set_required_features(risk_feature_names)

        if hasattr(decision, "set_required_features"):
            decision.set_required_features(decision_feature_names)
        # Bind semantic lookup index for fname()/fget() convenience (validation sets remain separate)
        if hasattr(model_main, "bind_feature_index"):
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

        execution_engine = ExecutionLoader.from_config(symbol=symbol, **cfg["execution"])
        portfolio = PortfolioLoader.from_config(symbol=symbol, **cfg["portfolio"])

        feature_extractor = FeatureLoader.from_config(
            final_features,
            data_handlers["ohlcv"],
            data_handlers.get("orderbook", {}),
            data_handlers.get("option_chain", {}),
            data_handlers.get("iv_surface", {}),
            data_handlers.get("sentiment", {}),
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
            Mapping[str, OHLCVDataHandler],
            data_handlers["ohlcv"]
        )
        orderbook_handlers = cast(
            Mapping[str, RealTimeOrderbookHandler],
            data_handlers.get("orderbook", {})
        )
        option_chain_handlers = cast(
            Mapping[str, OptionChainDataHandler],
            data_handlers.get("option_chain", {})
        )
        iv_surface_handlers = cast(
            Mapping[str, IVSurfaceDataHandler],
            data_handlers.get("iv_surface", {})
        )
        sentiment_handlers = cast(
            Mapping[str, SentimentDataHandler],
            data_handlers.get("sentiment", {})
        )

        spec = EngineSpec(
            mode=mode,
            symbol=symbol,
            interval=interval,
            interval_ms=int(interval_ms),
            universe=universe,
        )

        return StrategyEngine(
            spec=spec,
            ohlcv_handlers=ohlcv_handlers,
            orderbook_handlers=orderbook_handlers,
            option_chain_handlers=option_chain_handlers,
            iv_surface_handlers=iv_surface_handlers,
            sentiment_handlers=sentiment_handlers,
            feature_extractor=feature_extractor,
            models=models,
            decision=decision,
            risk_manager=risk_manager,
            execution_engine=execution_engine,
            portfolio_manager=portfolio
        )
