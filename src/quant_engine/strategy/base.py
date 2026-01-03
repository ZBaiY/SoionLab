"""
Strategy base (v4).
"""

from __future__ import annotations
from typing import ClassVar, Set, Optional, Dict, Any, Type, TypeVar
import copy

from quant_engine.data.contracts.protocol_realtime import to_interval_ms
from quant_engine.strategy.config import NormalizedStrategyCfg
from typing import cast

# ---------------------------------------------------------------------
# Global data presets registry (pure data semantics)
# ---------------------------------------------------------------------

GLOBAL_PRESETS: Dict[str, Any] = {
    # --- OHLCV ---
    "OHLCV_1M_30D": {
        "interval": "1m",
        # bootstrap/backfill horizon (realtime/mock convenience)
        "bootstrap": {"lookback": "30d"},
        "cache": {"max_bars": 10000},
        "columns": ["open", "high", "low", "close", "volume"],
    },
    "OHLCV_15M_180D": {
        "interval": "15m",
        # bootstrap/backfill horizon (realtime/mock convenience)
        "bootstrap": {"lookback": "180d"},
        "cache": {"max_bars": 10000},
        "columns": ["open", "high", "low", "close", "volume"],
        # columns: default view columns; storage always preserves full schema
    },

    # --- Orderbook ---
    "ORDERBOOK_L2_10_100MS": {
        "depth": 10,
        "aggregation": "L2",
        "interval": "100ms",
        "bootstrap": {"lookback": "1d"},
        "cache": {"max_bars": 10000},
    },
    "ORDERBOOK_L2_20_250MS": {
        "depth": 20,
        "aggregation": "L2",
        "interval": "250ms",
        "bootstrap": {"lookback": "1d"},
        "cache": {"max_bars": 10000},
    },

    # --- Option chain ---
    "OPTION_CHAIN_5M": {
        "interval": "5m",
        "bootstrap": {"lookback": "30d"},
        # OptionChainDataHandler cache uses maxlen/per_* and optional term bucketing.
        "cache": {
            "kind": "term",  # simple | expiry | term
            "maxlen": 512,
            "per_term_maxlen": 256,
            "term_bucket_ms": 86_400_000,  # 1d buckets by DTE
            "enable_expiry_index": True,
            "per_expiry_maxlen": 256,
        },
    },
    "OPTION_CHAIN_1M": {
        "interval": "1m",
        "bootstrap": {"lookback": "30d"},
        "cache": {
            "kind": "term",  # simple | expiry | term
            "maxlen": 1024,
            "per_term_maxlen": 512,
            "term_bucket_ms": 86_400_000,
            "enable_expiry_index": True,
            "per_expiry_maxlen": 512,
        },
    },

    # --- IV surface ---
    "IV_SURFACE_5M": {
        "interval": "5m",
        "calibrator": "SSVI",
        "bootstrap": {"lookback": "30d"},
        "cache": {"max_bars": 10000},
    },
    "IV_SURFACE_5M_FETCHED": {
        "interval": "5m",
        "calibrator": "FETCHED",
        "bootstrap": {"lookback": "30d"},
        "cache": {"max_bars": 10000},
    },
    "IV_SURFACE_1M": {
        "interval": "1m",
        "calibrator": "SSVI", # “SSVI”, “SABR”, "FETCHED"
        "bootstrap": {"lookback": "30d"},
        "cache": {"max_bars": 10000},
    },

    # --- Sentiment ---
    "SENTIMENT_BASIC_5M": {
        "interval": "5m",
        "model": "lexicon",
        "bootstrap": {"lookback": "30d"},
        "cache": {"max_bars": 10000},
    },
    "SENTIMENT_EMBEDDING_15M": {
        "interval": "15m",
        "model": "embedding",
        "bootstrap": {"lookback": "30d"},
        "cache": {"max_bars": 10000},
    },
    }


def register_global_preset(name: str, preset: Dict[str, Any], *, overwrite: bool = False) -> None:
    """Register a global `$ref` preset available to all strategies.

    Intended for shared handler templates (e.g., BINANCE_OHLCV_1M, DERIBIT_CHAIN_5M).
    """
    if not isinstance(name, str) or not name:
        raise TypeError("preset name must be a non-empty string")
    if not isinstance(preset, dict):
        raise TypeError("preset must be a dict")
    if (name in GLOBAL_PRESETS) and (not overwrite):
        raise KeyError(f"Global preset '{name}' already exists. Pass overwrite=True to replace.")
    GLOBAL_PRESETS[name] = copy.deepcopy(preset)


def get_global_presets() -> Dict[str, Any]:
    """Return a copy of all registered global presets."""
    return copy.deepcopy(GLOBAL_PRESETS)

T = TypeVar("T", bound="StrategyBase")


class StrategyBase:

    # Set by registry
    STRATEGY_NAME: ClassVar[str] = "UNREGISTERED"
    INTERVAL: ClassVar[str] = "1m"  # default observation interval

    # Optional: template/bound universe (B-style)
    UNIVERSE_TEMPLATE: ClassVar[Dict[str, Any]] = {}
    UNIVERSE: ClassVar[Dict[str, Any]] = {}

    # redundant, but making the class syntactically cleaner
    REQUIRED_DATA: ClassVar[Set[str]] = set()

    DATA: ClassVar[Dict[str, Any]] = {}
    FEATURES_USER: ClassVar[list[Dict[str, Any]]] = []
    MODEL_CFG: ClassVar[Optional[Dict[str, Any]]] = None
    DECISION_CFG: ClassVar[Optional[Dict[str, Any]]] = None
    RISK_CFG: ClassVar[Optional[Dict[str, Any]]] = None
    EXECUTION_CFG: ClassVar[Optional[Dict[str, Any]]] = None
    PORTFOLIO_CFG: ClassVar[Optional[Dict[str, Any]]] = None

    # Preset templates for semi-JSON specs (expanded in standardize via $ref)
    PRESETS: ClassVar[Dict[str, Any]] = {}

    def __init__(self) -> None:
        self._bound_symbols: Dict[str, str] | None = None

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """
        Construct a Strategy from a dict (e.g. JSON‑deserialized).
        """
        spec_name = None
        strategy_block = data.get("strategy")
        if isinstance(strategy_block, dict):
            name = strategy_block.get("name")
            if isinstance(name, str) and name:
                spec_name = name

        spec_cls = type(
            f"{cls.__name__}FromDict",
            (cls,),
            {
                "STRATEGY_NAME": spec_name or cls.STRATEGY_NAME,
                "INTERVAL": data.get("interval", cls.INTERVAL),
                "UNIVERSE_TEMPLATE": data.get("universe_template", {}),
                "UNIVERSE": data.get("universe", {}),
                "PRESETS": data.get("presets", {}),
                "REQUIRED_DATA": set(data.get("required_data", [])),
                "DATA": data.get("data", {}),
                "FEATURES_USER": data.get("features_user", []),
                "MODEL_CFG": data.get("model"),
                "DECISION_CFG": data.get("decision"),
                "RISK_CFG": data.get("risk"),
                "EXECUTION_CFG": data.get("execution"),
                "PORTFOLIO_CFG": data.get("portfolio"),
            },
        )
        return cast(T, spec_cls())

    @classmethod
    def validate_spec(cls) -> None:
        if not isinstance(cls.REQUIRED_DATA, set):
            raise TypeError("REQUIRED_DATA must be a set[str]")
        if not all(isinstance(x, str) for x in cls.REQUIRED_DATA):
            raise TypeError("REQUIRED_DATA must be a set[str]")
        if not cls.REQUIRED_DATA:
            raise ValueError("REQUIRED_DATA must be non-empty")

        if not isinstance(cls.DATA, dict):
            raise TypeError("DATA must be a dict")

        declared_domains: set[str] = set()

        primary = cls.DATA.get("primary", {})
        if not isinstance(primary, dict):
            raise TypeError("DATA['primary'] must be a dict")

        for domain in primary.keys():
            declared_domains.add(domain)

        secondary = cls.DATA.get("secondary", {})
        if secondary:
            if not isinstance(secondary, dict):
                raise TypeError("DATA['secondary'] must be a dict")
            for _, sec_block in secondary.items():
                if not isinstance(sec_block, dict):
                    raise TypeError("Each secondary symbol block must be a dict")
                for domain in sec_block.keys():
                    declared_domains.add(domain)

        missing = cls.REQUIRED_DATA - declared_domains
        if missing:
            raise ValueError(
                f"Strategy '{cls.STRATEGY_NAME}' requires data domains "
                f"{sorted(cls.REQUIRED_DATA)}, but DATA only declares "
                f"{sorted(declared_domains)} (missing {sorted(missing)})"
            )

        if not isinstance(cls.FEATURES_USER, list):
            raise TypeError("FEATURES_USER must be a list[dict]")
        for f in cls.FEATURES_USER:
            if not isinstance(f, dict):
                raise TypeError("Each FEATURES_USER entry must be a dict")

        for name, block in [
            ("MODEL_CFG", cls.MODEL_CFG),
            ("DECISION_CFG", cls.DECISION_CFG),
            ("RISK_CFG", cls.RISK_CFG),
            ("EXECUTION_CFG", cls.EXECUTION_CFG),
            ("PORTFOLIO_CFG", cls.PORTFOLIO_CFG),
        ]:
            if block is not None and not isinstance(block, dict):
                raise TypeError(f"{name} must be a dict or None")

    # =================================================================
    # B-style binding (template -> bound strategy instance)
    # =================================================================

    @staticmethod
    def _resolve_templates(obj: Any, symbols: Dict[str, str]) -> Any:
        """Resolve `{NAME}` placeholders recursively in nested specs."""
        if isinstance(obj, str):
            try:
                return obj.format(**symbols)
            except KeyError as e:
                raise KeyError(f"Missing bind symbol {e!s} for template string: {obj!r}") from e
        if isinstance(obj, list):
            return [StrategyBase._resolve_templates(x, symbols) for x in obj]
        if isinstance(obj, dict):
            return {StrategyBase._resolve_templates(k, symbols) if isinstance(k, str) else k:
                    StrategyBase._resolve_templates(v, symbols) for k, v in obj.items()}
        return obj

    @classmethod
    def bind_spec(cls, *, symbols: Dict[str, str]) -> Dict[str, Any]:
        """Return a bound spec dict with `{A}`, `{B}`, ... placeholders resolved."""
        cls.validate_spec()

        # Resolve universe first (if provided)
        if isinstance(cls.UNIVERSE_TEMPLATE, dict) and cls.UNIVERSE_TEMPLATE:
            universe = StrategyBase._resolve_templates(cls.UNIVERSE_TEMPLATE, symbols)
        elif cls.UNIVERSE:
            universe = copy.deepcopy(cls.UNIVERSE)
        else:
            universe = {}

        bound: Dict[str, Any] = {
            "universe": universe,
            "data": StrategyBase._resolve_templates(cls.DATA, symbols),
            "features_user": StrategyBase._resolve_templates(cls.FEATURES_USER, symbols),
            "model": StrategyBase._resolve_templates(cls.MODEL_CFG, symbols) if cls.MODEL_CFG else None,
            "decision": StrategyBase._resolve_templates(cls.DECISION_CFG, symbols) if cls.DECISION_CFG else None,
            "risk": StrategyBase._resolve_templates(cls.RISK_CFG, symbols) if cls.RISK_CFG else None,
            "execution": StrategyBase._resolve_templates(cls.EXECUTION_CFG, symbols) if cls.EXECUTION_CFG else None,
            "portfolio": StrategyBase._resolve_templates(cls.PORTFOLIO_CFG, symbols) if cls.PORTFOLIO_CFG else None,
        }
        return bound

    def bind(self: T, **symbols: str) -> T:
        """Return a *new* Strategy instance with `{A}`, `{B}`, ... placeholders resolved."""
        bound: T = self.__class__()
        bound._bound_symbols = dict(symbols)

        bound_spec = self.__class__.bind_spec(symbols=bound._bound_symbols)

        # Backward compatibility for existing loader: expose primary symbol if available.
        primary = None
        universe = bound_spec.get("universe")
        if isinstance(universe, dict):
            primary = universe.get("primary")
        if isinstance(primary, str) and primary:
            setattr(bound, "SYMBOL", primary)

        return bound

    # =================================================================
    # Spec standardization (semi-JSON -> normalized cfg)
    # =================================================================

    @staticmethod
    def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        """Recursive dict merge; overlay wins."""
        out: Dict[str, Any] = copy.deepcopy(base)
        for k, v in overlay.items():
            if (
                k in out
                and isinstance(out[k], dict)
                and isinstance(v, dict)
            ):
                out[k] = StrategyBase._deep_merge(out[k], v)
            else:
                out[k] = copy.deepcopy(v)
        return out

    @staticmethod
    def _expand_refs(obj: Any, presets: Dict[str, Any]) -> Any:
        """Expand dict objects containing `$ref` recursively.

        Supported form:
            {"$ref": "NAME", ...overrides}
        where `presets[NAME]` must be a dict.
        """
        if isinstance(obj, list):
            return [StrategyBase._expand_refs(x, presets) for x in obj]

        if isinstance(obj, dict):
            if "$ref" in obj:
                ref_name = obj.get("$ref")
                if not isinstance(ref_name, str) or not ref_name:
                    raise TypeError(f"$ref must be a non-empty string, got {ref_name!r}")
                if ref_name not in presets:
                    raise KeyError(f"Unknown preset '{ref_name}'. Available: {sorted(presets.keys())}")
                preset = presets[ref_name]
                if not isinstance(preset, dict):
                    raise TypeError(f"Preset '{ref_name}' must be a dict, got {type(preset).__name__}")

                overrides = {k: v for k, v in obj.items() if k != "$ref"}
                merged = StrategyBase._deep_merge(preset, overrides)
                return StrategyBase._expand_refs(merged, presets)

            return {k: StrategyBase._expand_refs(v, presets) for k, v in obj.items()}

        return obj

    @staticmethod
    def _canonical_feature_name(
        *,
        ftype: str,
        purpose: str,
        symbol: str,
        ref: str | None = None,
    ) -> str:
        """Canonical feature name: TYPE_PURPOSE_SYMBOL[^REF]."""
        base = f"{ftype}_{purpose}_{symbol}"
        return f"{base}^{ref}" if ref else base

    @classmethod
    def standardize(
        cls,
        overrides: Dict[str, Any] | None = None,
        *,
        symbols: Dict[str, str] | None = None,
    ) -> NormalizedStrategyCfg:
        """
        Return a normalized cfg dataclass consumed by StrategyLoader.
        """
        cls.validate_spec()

        bound_spec = None
        if symbols is not None:
            bound_spec = cls.bind_spec(symbols=symbols)

        merged = cls.apply_defaults(overrides or {}, spec=bound_spec)
        merged.setdefault("interval", cls.INTERVAL)

        # Guard: time-range & warmup are runtime/driver concerns, never strategy config.
        forbidden = {"start_ts", "end_ts", "warmup_steps", "warmup_to", "warmup"}
        present = forbidden.intersection(set(merged.keys()))
        if present:
            raise ValueError(f"Forbidden runtime keys in Strategy.standardize(): {sorted(present)}")

        out: Dict[str, Any] = copy.deepcopy(merged)
        out.setdefault("required_data", sorted(cls.REQUIRED_DATA))

        if bound_spec and isinstance(bound_spec.get("universe"), dict) and bound_spec.get("universe"):
            out.setdefault("universe", copy.deepcopy(bound_spec.get("universe")))
        elif isinstance(cls.UNIVERSE, dict) and cls.UNIVERSE:
            out.setdefault("universe", copy.deepcopy(cls.UNIVERSE))

        # Prefer bound universe primary symbol when available; fallback to legacy SYMBOL.
        primary_symbol = None
        universe = out.get("universe")
        if isinstance(universe, dict) and universe:
            primary_symbol = universe.get("primary")
        if not primary_symbol:
            primary_symbol = getattr(cls, "SYMBOL", None)

        out.setdefault("symbol", primary_symbol)

        interval = out.get("interval")
        if not isinstance(interval, str) or not interval:
            interval = cls.INTERVAL
            out["interval"] = interval
        if isinstance(interval, str) and interval:
            if "interval_ms" not in out or out.get("interval_ms") is None:
                ms = to_interval_ms(interval)
                if ms is None:
                    raise ValueError(f"Invalid interval format: {interval}")
                out["interval_ms"] = int(ms)

        # Merge precedence: global -> strategy -> runtime
        combined_presets: Dict[str, Any] = copy.deepcopy(GLOBAL_PRESETS)
        if isinstance(cls.PRESETS, dict) and cls.PRESETS:
            for k, v in cls.PRESETS.items():
                combined_presets[k] = copy.deepcopy(v)

        # Runtime-provided presets override strategy
        runtime_presets = out.get("presets")
        if isinstance(runtime_presets, dict) and runtime_presets:
            for k, v in runtime_presets.items():
                combined_presets[k] = v
        if combined_presets:
            out = StrategyBase._expand_refs(out, combined_presets)
        out.pop("presets", None)

        # ---------------------------
        # DATA: unify interval + remove history.warmup
        # ---------------------------

        data = out.get("data")
        if isinstance(data, dict):
            # ------ helper ------
            def _fix_ohlcv(block: Dict[str, Any]) -> None:
                # unify interval key
                if "tf" in block and "interval" not in block:
                    block["interval"] = block.pop("tf")
                interval = block.get("interval")
                if isinstance(interval, str) and interval:
                    ms = to_interval_ms(interval)
                    if ms is None:
                        raise ValueError(f"Invalid interval format: {interval}")
                    block["interval_ms"] = int(ms)
                # strip warmup from history (runtime concern)
                hist = block.get("history")
                if isinstance(hist, dict):
                    hist.pop("warmup", None)

            def _fix_orderbook(block: Dict[str, Any]) -> None:
                # unify interval key
                if "tf" in block and "interval" not in block:
                    block["interval"] = block.pop("tf")
                if "refresh_interval" in block and "interval" not in block:
                    block["interval"] = block.pop("refresh_interval")
                interval = block.get("interval")
                if isinstance(interval, str) and interval:
                    ms = to_interval_ms(interval)
                    if ms is None:
                        raise ValueError(f"Invalid interval format: {interval}")
                    block["interval_ms"] = int(ms)
                # strip warmup from history (runtime concern)
                hist = block.get("history")
                if isinstance(hist, dict):
                    hist.pop("warmup", None)

            def _fix_generic(block: Dict[str, Any]) -> None:
                if "tf" in block and "interval" not in block:
                    block["interval"] = block.pop("tf")
                interval = block.get("interval")
                if isinstance(interval, str) and interval:
                    ms = to_interval_ms(interval)
                    if ms is None:
                        raise ValueError(f"Invalid interval format: {interval}")
                    block["interval_ms"] = int(ms)
                hist = block.get("history")
                if isinstance(hist, dict):
                    hist.pop("warmup", None)
            # ------ process ------
            primary = data.get("primary")
            if isinstance(primary, dict):
                ohlcv = primary.get("ohlcv")
                if isinstance(ohlcv, dict):
                    _fix_ohlcv(ohlcv)
                orderbook = primary.get("orderbook")
                if isinstance(orderbook, dict):
                    _fix_orderbook(orderbook)
                option_chain = primary.get("option_chain")
                if isinstance(option_chain, dict):
                    _fix_generic(option_chain)
                iv_surface = primary.get("iv_surface")
                if isinstance(iv_surface, dict):
                    _fix_generic(iv_surface)
                sentiment = primary.get("sentiment")
                if isinstance(sentiment, dict):
                    _fix_generic(sentiment)
            
            # ------ secondary ------
            secondary = data.get("secondary")
            if isinstance(secondary, dict):
                for _, sec in secondary.items():
                    if not isinstance(sec, dict):
                        continue
                    ohlcv = sec.get("ohlcv")
                    if isinstance(ohlcv, dict):
                        _fix_ohlcv(ohlcv)
                    orderbook = sec.get("orderbook")
                    if isinstance(orderbook, dict):
                        _fix_orderbook(orderbook)
                    option_chain = sec.get("option_chain")
                    if isinstance(option_chain, dict):
                        _fix_generic(option_chain)
                    iv_surface = sec.get("iv_surface")
                    if isinstance(iv_surface, dict):
                        _fix_generic(iv_surface)
                    sentiment = sec.get("sentiment")
                    if isinstance(sentiment, dict):
                        _fix_generic(sentiment)

        # ---------------------------
        # FEATURES: unify params.ref + canonicalize names
        # ---------------------------
        feats = out.get("features_user")
        if isinstance(feats, list):
            norm: list[Dict[str, Any]] = []
            for raw in feats:
                if not isinstance(raw, dict):
                    continue
                f = copy.deepcopy(raw)

                ftype = str(f.get("type") or "").upper()
                symbol = str(f.get("symbol") or "")

                params = f.get("params")
                if not isinstance(params, dict):
                    params = {}
                # unify secondary -> ref
                if "secondary" in params and "ref" not in params:
                    params["ref"] = params.pop("secondary")
                if "ref_symbol" in params and "ref" not in params:
                    params["ref"] = params.pop("ref_symbol")
                f["params"] = params

                # purpose: allow explicit key; else infer from existing name; else default MODEL
                purpose = str(f.get("purpose") or "").upper()
                if not purpose:
                    name = f.get("name")
                    if isinstance(name, str) and name:
                        parts = name.split("_", 2)
                        if len(parts) >= 2 and parts[1]:
                            purpose = str(parts[1]).upper()
                if not purpose:
                    purpose = "MODEL"

                ref = params.get("ref")
                ref_s: str | None = None
                if ref is not None:
                    ref_s = str(ref)

                # enforce canonical naming convention
                f["name"] = cls._canonical_feature_name(
                    ftype=ftype,
                    purpose=purpose,
                    symbol=symbol,
                    ref=ref_s,
                )

                # remove optional 'purpose' to keep cfg schema stable
                f.pop("purpose", None)

                norm.append(f)

            out["features_user"] = norm

        # ---------------------------
        # MODEL/DECISION/RISK: unify params.secondary -> params.ref (soft)
        # ---------------------------
        for key in ("model", "decision"):
            blk = out.get(key)
            if isinstance(blk, dict):
                params = blk.get("params")
                if isinstance(params, dict) and "secondary" in params and "ref" not in params:
                    params["ref"] = params.pop("secondary")

        risk = out.get("risk")
        if isinstance(risk, dict):
            rules = risk.get("rules")
            if isinstance(rules, dict):
                for _, rule in rules.items():
                    if not isinstance(rule, dict):
                        continue
                    params = rule.get("params")
                    if isinstance(params, dict) and "secondary" in params and "ref" not in params:
                        params["ref"] = params.pop("secondary")

        interval = out.get("interval")
        interval_ms = out.get("interval_ms")

        return NormalizedStrategyCfg(
            strategy_name=cls.STRATEGY_NAME,
            interval=str(interval) if interval is not None else cls.INTERVAL,
            interval_ms=int(interval_ms) if interval_ms is not None else None,
            required_data=tuple(out.get("required_data") or ()),
            data=out.get("data") or {},
            features_user=out.get("features_user") or [],
            model=out.get("model"),
            decision=out.get("decision"),
            risk=out.get("risk"),
            execution=out.get("execution"),
            portfolio=out.get("portfolio"),
            universe=out.get("universe"),
            symbol=out.get("symbol"),
        )


    @classmethod
    def apply_defaults(
        cls,
        cfg: Dict[str, Any],
        *,
        spec: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """
        Merge Strategy-declared defaults into a runtime cfg dict.

        Explicit values in cfg always win.
        """
        merged = copy.deepcopy(cfg)

        def _pick(key: str, fallback: Any) -> Any:
            if spec is not None and key in spec:
                return spec.get(key)
            return fallback

        features_user = _pick("features_user", cls.FEATURES_USER)
        if features_user:
            merged.setdefault("features_user", features_user)

        model_cfg = _pick("model", cls.MODEL_CFG)
        if model_cfg:
            merged.setdefault("model", model_cfg)

        decision_cfg = _pick("decision", cls.DECISION_CFG)
        if decision_cfg:
            merged.setdefault("decision", decision_cfg)

        risk_cfg = _pick("risk", cls.RISK_CFG)
        if risk_cfg:
            merged.setdefault("risk", risk_cfg)

        execution_cfg = _pick("execution", cls.EXECUTION_CFG)
        if execution_cfg:
            merged.setdefault("execution", execution_cfg)

        portfolio_cfg = _pick("portfolio", cls.PORTFOLIO_CFG)
        if portfolio_cfg:
            merged.setdefault("portfolio", portfolio_cfg)

        presets = _pick("presets", cls.PRESETS)
        if presets:
            merged.setdefault("presets", copy.deepcopy(presets))

        data = _pick("data", cls.DATA)
        if data:
            merged.setdefault("data", copy.deepcopy(data))

        return merged

    def to_dict(self) -> Dict[str, Any]:
        """
        Export this Strategy specification to a JSON‑serializable dict.
        """
        spec = None
        if self._bound_symbols:
            spec = self.__class__.bind_spec(symbols=self._bound_symbols)

        universe = spec.get("universe") if spec else copy.deepcopy(self.__class__.UNIVERSE)
        data = spec.get("data") if spec else copy.deepcopy(self.__class__.DATA)
        features_user = spec.get("features_user") if spec else copy.deepcopy(self.__class__.FEATURES_USER)
        model_cfg = spec.get("model") if spec else copy.deepcopy(self.__class__.MODEL_CFG)
        decision_cfg = spec.get("decision") if spec else copy.deepcopy(self.__class__.DECISION_CFG)
        risk_cfg = spec.get("risk") if spec else copy.deepcopy(self.__class__.RISK_CFG)
        execution_cfg = spec.get("execution") if spec else copy.deepcopy(self.__class__.EXECUTION_CFG)
        portfolio_cfg = spec.get("portfolio") if spec else copy.deepcopy(self.__class__.PORTFOLIO_CFG)

        return {
            "strategy": {"name": self.__class__.STRATEGY_NAME},
            "universe_template": copy.deepcopy(self.__class__.UNIVERSE_TEMPLATE),
            "universe": copy.deepcopy(universe),
            "presets": copy.deepcopy(self.__class__.PRESETS),
            "required_data": sorted(self.__class__.REQUIRED_DATA),
            "data": copy.deepcopy(data),
            "features_user": copy.deepcopy(features_user),
            "model": copy.deepcopy(model_cfg),
            "decision": copy.deepcopy(decision_cfg),
            "risk": copy.deepcopy(risk_cfg),
            "execution": copy.deepcopy(execution_cfg),
            "portfolio": copy.deepcopy(portfolio_cfg),
        }

    

    def build(self, mode, overrides: Dict[str, Any] | None = None):
        """Build a StrategyEngine using StrategyLoader.

        Args:
            mode: Engine mode (e.g., EngineMode.BACKTEST / REALTIME / MOCK).
            overrides: Runtime config overrides (does not mutate the Strategy).
        """
        if mode is None:
            raise ValueError("StrategyBase.build(mode=...) requires a non-None mode")

        # Local import to avoid import-time coupling/cycles.
        from quant_engine.strategy.loader import StrategyLoader

        return StrategyLoader.from_config(
            strategy=self,
            mode=mode,
            overrides=overrides or {},
        )
