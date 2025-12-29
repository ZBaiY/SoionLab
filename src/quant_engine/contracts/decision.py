from typing import Protocol, Dict, Any, Iterable, Tuple


# --- Feature name parsing and semantic lookup helpers ---
FeatureKey = Tuple[str, str, str, str | None]


def parse_feature_name(name: str) -> FeatureKey:
    """Parse v4 feature naming convention.

    Expected:
        <TYPE>_<PURPOSE>_<SYMBOL>
        <TYPE>_<PURPOSE>_<SYMBOL>^<REF>

    Example:
        RSI_MODEL_BTCUSDT
        SPREAD_DECISION_BTCUSDT^ETHUSDT

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


class DecisionProto(Protocol):
    """
    V4 unified decision protocol:
        • decide(context) -> float
        • context contains model_score, features, sentiment, regime, portfolio_state
        • decision module should NOT know symbol
        • decision should not filter features; model/risk handle that
    """

    def decide(self, context: Dict[str, Any]) -> float:
        ...

    required_feature_types: set[str]
    required_features: set[str]


# ----------------------------------------------------------------------
# V4 Decision Base Class
# ----------------------------------------------------------------------
class DecisionBase(DecisionProto):
    """
    Unified base class for decision modules.

    Key properties in v4:
        • symbol-agnostic (symbol filtering belongs to ModelBase / RiskBase)
        • may optionally depend on features (explicitly declared)
        • must implement decide(context)
    """

    # Validation contracts (loader injects required feature NAMES for completeness checks)
    required_feature_types: set[str] = set()   # design-time contract (feature types)
    required_features: set[str] = set()        # validation-only required feature NAMES
    symbol: str | None = None

    def __init__(self, symbol: str | None = None, **kwargs):
        self.symbol = symbol
        # Decisions do not need symbol/secondary.
        # Copy class-level declarations to instance to avoid shared-state surprises.
        self.required_feature_types = set(type(self).required_feature_types)
        self.required_features = set(type(self).required_features)


    def decide(self, context: Dict[str, Any]) -> float:
        raise NotImplementedError("Decision module must implement decide()")

    def validate_feature_types(self, available_feature_types: set[str]) -> None:
        missing = self.required_feature_types - available_feature_types
        if missing:
            raise ValueError(
                f"{type(self).__name__} missing required feature types: {sorted(missing)}. "
                f"Available feature types: {sorted(available_feature_types)}"
            )

    def set_required_features(self, feature_names: Iterable[str]) -> None:
        # Validation-only: used by engine/loader to verify runtime completeness.
        self.required_features = set(feature_names)

    def bind_feature_index(self, available_feature_names: Iterable[str]) -> None:
        """Bind a semantic name index for decision-side convenience."""
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
        purpose: str = "DECISION",
        symbol: str | None = None,
        ref: str | None = None,
    ) -> str:
        """Resolve a feature name by semantic key.

        Decisions are symbol-agnostic, so `symbol` must be provided unless the
        caller uses purpose-only features that encode a fixed symbol.
        """
        if symbol is None:
            raise ValueError("DecisionBase.fname(...) requires symbol=... (decisions are symbol-agnostic)")
        key: FeatureKey = (ftype, purpose, symbol, ref)
        if key in self._feature_index:
            return self._feature_index[key]

        candidates = [
            n
            for (t, p, s, r), n in self._feature_index.items()
            if t == ftype and p == purpose and s == symbol
        ]
        if candidates:
            raise KeyError(
                f"No feature for key={key}. Candidates for (TYPE={ftype}, PURPOSE={purpose}, SYMBOL={symbol}): {sorted(candidates)}"
            )
        raise KeyError(f"No feature for key={key}. Decision has {len(self._feature_index)} indexed features.")


    def fget(
        self,
        features: Dict[str, Any],
        ftype: str,
        purpose: str = "DECISION",
        symbol: str | None = None,
        ref: str | None = None,
    ) -> Any:
        """Convenience getter: features[fname(...)]"""
        return features[self.fname(ftype=ftype, purpose=purpose, symbol=symbol, ref=ref)]

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