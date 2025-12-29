from typing import Protocol, Dict, Any, List, Iterable, Tuple

SCHEMA_VERSION = 2


# --- Feature name parsing and semantic lookup helpers ---

FeatureKey = Tuple[str, str, str, str | None]


def parse_feature_name(name: str) -> FeatureKey:
    """Parse v4 feature naming convention.

    Expected:
        <TYPE>_<PURPOSE>_<SYMBOL>
        <TYPE>_<PURPOSE>_<SYMBOL>^<REF>

    Example:
        RSI_MODEL_BTCUSDT
        SPREAD_MODEL_BTCUSDT^ETHUSDT

    Returns:
        (ftype, purpose, symbol, ref)
    """
    parts = name.split("_", 2)
    if len(parts) != 3:
        raise ValueError(
            f"Invalid feature name '{name}': expected '<TYPE>_<PURPOSE>_<SYMBOL>' or '<TYPE>_<PURPOSE>_<SYMBOL>^<REF>'"
        )
    ftype, purpose, sym_part = parts
    ref: str | None = None
    symbol = sym_part
    if "^" in sym_part:
        symbol, ref = sym_part.split("^", 1)
        if not symbol or not ref:
            raise ValueError(f"Invalid feature name '{name}': malformed '^' section")
    if not ftype or not purpose or not symbol:
        raise ValueError(f"Invalid feature name '{name}': empty TYPE/PURPOSE/SYMBOL")
    return (ftype, purpose, symbol, ref)


class ModelProto(Protocol):
    """
    V4 unified model protocol:
        • models declare required feature names explicitly
        • symbols are informational (routing / execution), not feature identity
    """

    symbol: str
    secondary: str | None
    required_features: List[str]
    required_feature_types: set[str]

    def predict(self, features: Dict[str, Any]) -> float:
        ...


# ----------------------------------------------------------------------
# V4 Model Base Class
# ----------------------------------------------------------------------
class ModelBase(ModelProto):
    """
    V4 unified base class for all models used in the engine.

    Responsibilities:
        • store primary symbol (self.symbol)
        • optionally store secondary symbol
        • declare required feature names (explicit, immutable)
        • implement predict(features)
    """

    # Load-time validation contracts (injected/checked before runtime)
    required_features: set[str] = set()          # required feature NAMES (validation only)
    required_feature_types: set[str] = set()     # required feature TYPES (design-time contract)

    # Runtime identity
    symbol: str                                  # primary trading symbol
    secondary: str | None = None                 # optional secondary

    def __init__(self, symbol: str, **kwargs):
        self.symbol = symbol
        self.secondary = kwargs.get("secondary", None)

        # Validation sets (populated/overridden by loader)
        self.required_features = set()
        self.required_feature_types = type(self).required_feature_types

        # Semantic lookup index: (TYPE, PURPOSE, SYMBOL, REF) -> feature_name
        self._feature_index: dict[FeatureKey, str] = {}

    def set_required_features(self, feature_names: Iterable[str]) -> None:
        # Validation-only: used by engine/loader to verify runtime completeness.
        self.required_features = set(feature_names)


    def bind_feature_index(self, available_feature_names: Iterable[str]) -> None:
        """Bind a semantic name index for model-side convenience.

        This does NOT replace `required_features` validation; it just lets models
        access features via semantic queries instead of hardcoding string keys.
        """
        index: dict[FeatureKey, str] = {}
        for name in available_feature_names:
            key = parse_feature_name(name)
            if key in index and index[key] != name:
                raise ValueError(f"Feature key collision for {key}: '{index[key]}' vs '{name}'")
            index[key] = name
        self._feature_index = index


    def fname(
        self,
        ftype: str,
        purpose: str = "MODEL",
        symbol: str | None = None,
        ref: str | None = None,
    ) -> str:
        """Resolve a feature name by semantic key."""
        sym = symbol or self.symbol
        key: FeatureKey = (ftype, purpose, sym, ref)
        if key in self._feature_index:
            return self._feature_index[key]

        # Helpful diagnostics: show close candidates
        candidates = [
            n
            for (t, p, s, r), n in self._feature_index.items()
            if t == ftype and p == purpose and s == sym
        ]
        if candidates:
            raise KeyError(
                f"No feature for key={key}. Candidates for (TYPE={ftype}, PURPOSE={purpose}, SYMBOL={sym}): {sorted(candidates)}"
            )
        raise KeyError(f"No feature for key={key}. Model has {len(self._feature_index)} indexed features.")


    def fget(
        self,
        features: Dict[str, Any],
        ftype: str,
        purpose: str = "MODEL",
        symbol: str | None = None,
        ref: str | None = None,
    ) -> Any:
        """Convenience getter: features[fname(...)]"""
        return features[self.fname(ftype=ftype, purpose=purpose, symbol=symbol, ref=ref)]


    # ------------------------------------------------------------------
    # Child models must implement this
    # ------------------------------------------------------------------
    def predict(self, features: Dict[str, Any]) -> float:
        raise NotImplementedError("Model must implement predict()")

    def predict_with_context(self, features: Dict[str, Any], context: Dict[str, Any]) -> float:
        return self.predict(features)

    def market_status(self, context: Dict[str, Any]) -> str | None:
        market = context.get("market_data")
        if isinstance(market, dict):
            status = market.get("market", {}).get("status") if isinstance(market.get("market"), dict) else None
            if status is not None:
                return str(status)
        snapshots = context.get("market_snapshots")
        if isinstance(snapshots, dict):
            ohlcv = snapshots.get("ohlcv")
            if isinstance(ohlcv, dict):
                snap = ohlcv.get(self.symbol) if self.symbol is not None else next(iter(ohlcv.values()), None)
                if snap is not None:
                    market = getattr(snap, "market", None)
                    status = getattr(market, "status", None)
                    if status is not None:
                        return str(status)
        return None

    def market_is_active(self, context: Dict[str, Any]) -> bool:
        status = self.market_status(context)
        if status is None:
            return True
        return str(status).lower() == "open"

    def validate_feature_types(self, available_feature_types: set[str]) -> None:
        """
        Validate that all required feature TYPES declared by the model
        are provided by the strategy configuration.

        This is a design-time cross-check only.
        No name inference or symbol logic is allowed here.
        """
        missing = self.required_feature_types - available_feature_types
        if missing:
            raise ValueError(
                f"{type(self).__name__} missing required feature types: {sorted(missing)}. "
                f"Available feature types: {sorted(available_feature_types)}"
            )
    def validate_features(self, available_features: set[str]) -> None:
        """
        Validate that all required feature NAMES are present at runtime.
        """
        missing = [f for f in self.required_features if f not in available_features]
        if missing:
            raise ValueError(
                f"{type(self).__name__} missing required features: {missing}. "
                f"Available features: {sorted(available_features)}"
            )
