"""
Strategy base (v4).
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Set, Optional, Dict, Any, Type, TypeVar
import copy

from quant_engine.data.contracts.protocol_realtime import to_interval_ms

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
        "cache": {"max_bars": 10000},
    },
    "OPTION_CHAIN_1M": {
        "interval": "1m",
        "bootstrap": {"lookback": "30d"},
        "cache": {"max_bars": 10000},
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

@dataclass
class StrategyBase:

    # Set by registry
    STRATEGY_NAME: str = "UNREGISTERED"
    INTERVAL: str = "1m"  # default observation interval

    # Optional: template/bound universe (B-style)
    UNIVERSE_TEMPLATE: Dict[str, Any] = field(default_factory=dict)
    UNIVERSE: Dict[str, Any] = field(default_factory=dict)

    # redundant, but making the class syntactically cleaner
    REQUIRED_DATA: Set[str] = field(default_factory=set) 

    DATA: Dict[str, Any] = field(default_factory=dict)
    FEATURES_USER: list[Dict[str, Any]] = field(default_factory=list)
    MODEL_CFG: Optional[Dict[str, Any]] = None
    DECISION_CFG: Optional[Dict[str, Any]] = None
    RISK_CFG: Optional[Dict[str, Any]] = None
    EXECUTION_CFG: Optional[Dict[str, Any]] = None
    PORTFOLIO_CFG: Optional[Dict[str, Any]] = None

    # Preset templates for semi-JSON specs (expanded in standardize via $ref)
    PRESETS: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.validate()

    def validate(self) -> None:
        if not isinstance(self.REQUIRED_DATA, set):
            raise TypeError("REQUIRED_DATA must be a set[str]")
        if not all(isinstance(x, str) for x in self.REQUIRED_DATA):
            raise TypeError("REQUIRED_DATA must be a set[str]")
        if not self.REQUIRED_DATA:
            raise ValueError("REQUIRED_DATA must be non-empty")

        if not isinstance(self.DATA, dict):
            raise TypeError("DATA must be a dict")

        declared_domains: set[str] = set()

        primary = self.DATA.get("primary", {})
        if not isinstance(primary, dict):
            raise TypeError("DATA['primary'] must be a dict")

        for domain in primary.keys():
            declared_domains.add(domain)

        secondary = self.DATA.get("secondary", {})
        if secondary:
            if not isinstance(secondary, dict):
                raise TypeError("DATA['secondary'] must be a dict")
            for _, sec_block in secondary.items():
                if not isinstance(sec_block, dict):
                    raise TypeError("Each secondary symbol block must be a dict")
                for domain in sec_block.keys():
                    declared_domains.add(domain)

        missing = self.REQUIRED_DATA - declared_domains
        if missing:
            raise ValueError(
                f"Strategy '{self.STRATEGY_NAME}' requires data domains "
                f"{sorted(self.REQUIRED_DATA)}, but DATA only declares "
                f"{sorted(declared_domains)} (missing {sorted(missing)})"
            )

        if not isinstance(self.FEATURES_USER, list):
            raise TypeError("FEATURES_USER must be a list[dict]")
        for f in self.FEATURES_USER:
            if not isinstance(f, dict):
                raise TypeError("Each FEATURES_USER entry must be a dict")

        for name, block in [
            ("MODEL_CFG", self.MODEL_CFG),
            ("DECISION_CFG", self.DECISION_CFG),
            ("RISK_CFG", self.RISK_CFG),
            ("EXECUTION_CFG", self.EXECUTION_CFG),
            ("PORTFOLIO_CFG", self.PORTFOLIO_CFG),
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

    def bind(self: T, **symbols: str) -> T:
        """Return a *new* Strategy instance with `{A}`, `{B}`, ... placeholders resolved.

        Binding is purely structural: it resolves template placeholders in DATA / FEATURES / CFG blocks.
        It MUST NOT introduce runtime time-range concepts (start_ts/end_ts/warmup, etc.).
        """
        bound: T = copy.deepcopy(self)

        # Resolve universe first (if provided)
        if isinstance(bound.UNIVERSE_TEMPLATE, dict) and bound.UNIVERSE_TEMPLATE:
            bound.UNIVERSE = StrategyBase._resolve_templates(bound.UNIVERSE_TEMPLATE, symbols)
        elif not bound.UNIVERSE:
            bound.UNIVERSE = {}

        # Resolve the rest of the semi-JSON spec blocks
        bound.DATA = StrategyBase._resolve_templates(bound.DATA, symbols)
        bound.FEATURES_USER = StrategyBase._resolve_templates(bound.FEATURES_USER, symbols)
        bound.MODEL_CFG = StrategyBase._resolve_templates(bound.MODEL_CFG, symbols) if bound.MODEL_CFG else None
        bound.DECISION_CFG = StrategyBase._resolve_templates(bound.DECISION_CFG, symbols) if bound.DECISION_CFG else None
        bound.RISK_CFG = StrategyBase._resolve_templates(bound.RISK_CFG, symbols) if bound.RISK_CFG else None
        bound.EXECUTION_CFG = StrategyBase._resolve_templates(bound.EXECUTION_CFG, symbols) if bound.EXECUTION_CFG else None
        bound.PORTFOLIO_CFG = StrategyBase._resolve_templates(bound.PORTFOLIO_CFG, symbols) if bound.PORTFOLIO_CFG else None

        # Backward compatibility for existing loader: expose primary symbol if available.
        # (Loader will be upgraded later to prefer UNIVERSE.)
        primary = None
        if isinstance(bound.UNIVERSE, dict):
            primary = bound.UNIVERSE.get("primary")
        if isinstance(primary, str) and primary:
            setattr(bound, "SYMBOL", primary)

        bound.validate()
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

    def standardize(self, cfg: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """
        Return a normalized cfg dict consumed by StrategyLoader.
        """
        merged = self.apply_defaults(cfg or {})

        # Guard: time-range & warmup are runtime/driver concerns, never strategy config.
        forbidden = {"start_ts", "end_ts", "warmup_steps", "warmup_to", "warmup"}
        present = forbidden.intersection(set(merged.keys()))
        if present:
            raise ValueError(f"Forbidden runtime keys in Strategy.standardize(): {sorted(present)}")

        out: Dict[str, Any] = copy.deepcopy(merged)
        out.setdefault("required_data", sorted(self.REQUIRED_DATA))
        # Prefer bound universe primary symbol when available; fallback to legacy SYMBOL.
        primary_symbol = None
        if isinstance(getattr(self, "UNIVERSE", None), dict) and self.UNIVERSE:
            primary_symbol = self.UNIVERSE.get("primary")
        if not primary_symbol:
            primary_symbol = getattr(self, "SYMBOL", None)

        out.setdefault("symbol", primary_symbol)
        if isinstance(getattr(self, "UNIVERSE", None), dict) and self.UNIVERSE:
            out.setdefault("universe", copy.deepcopy(self.UNIVERSE))

        # Merge precedence: global -> strategy -> runtime
        combined_presets: Dict[str, Any] = copy.deepcopy(GLOBAL_PRESETS)
        if isinstance(self.PRESETS, dict) and self.PRESETS:
            for k, v in self.PRESETS.items():
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
                f["name"] = self._canonical_feature_name(
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

        return out


    def apply_defaults(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge Strategy-declared defaults into a runtime cfg dict.

        Explicit values in cfg always win.
        """
        merged = copy.deepcopy(cfg)

        if self.FEATURES_USER:
            merged.setdefault("features_user", self.FEATURES_USER)

        if self.MODEL_CFG:
            merged.setdefault("model", self.MODEL_CFG)

        if self.DECISION_CFG:
            merged.setdefault("decision", self.DECISION_CFG)

        if self.RISK_CFG:
            merged.setdefault("risk", self.RISK_CFG)

        if self.EXECUTION_CFG:
            merged.setdefault("execution", self.EXECUTION_CFG)

        if self.PORTFOLIO_CFG:
            merged.setdefault("portfolio", self.PORTFOLIO_CFG)

        if self.PRESETS:
            merged.setdefault("presets", copy.deepcopy(self.PRESETS))

        if self.DATA:
            merged.setdefault("data", copy.deepcopy(self.DATA))

        return merged

    def to_dict(self) -> Dict[str, Any]:
        """
        Export this Strategy specification to a JSON‑serializable dict.
        """
        return {
            "strategy": {"name": self.STRATEGY_NAME},
            "universe_template": copy.deepcopy(self.UNIVERSE_TEMPLATE),
            "universe": copy.deepcopy(self.UNIVERSE),
            "presets": copy.deepcopy(self.PRESETS),
            "required_data": sorted(self.REQUIRED_DATA),
            "data": copy.deepcopy(self.DATA),
            "features_user": copy.deepcopy(self.FEATURES_USER),
            "model": copy.deepcopy(self.MODEL_CFG),
            "decision": copy.deepcopy(self.DECISION_CFG),
            "risk": copy.deepcopy(self.RISK_CFG),
            "execution": copy.deepcopy(self.EXECUTION_CFG),
            "portfolio": copy.deepcopy(self.PORTFOLIO_CFG),
        }

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """
        Construct a Strategy from a dict (e.g. JSON‑deserialized).
        """
        return cls(
            UNIVERSE_TEMPLATE=data.get("universe_template", {}),
            UNIVERSE=data.get("universe", {}),
            PRESETS=data.get("presets", {}),
            REQUIRED_DATA=set(data.get("required_data", [])),
            DATA=data.get("data", {}),
            FEATURES_USER=data.get("features_user", []),
            MODEL_CFG=data.get("model"),
            DECISION_CFG=data.get("decision"),
            RISK_CFG=data.get("risk"),
            EXECUTION_CFG=data.get("execution"),
            PORTFOLIO_CFG=data.get("portfolio"),
        )

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
