from __future__ import annotations
from typing import List, Dict, Any


# ----------------------------------------------------------------------
# Deduplication
# ----------------------------------------------------------------------
def _dedupe(features: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplicate feature specs by (name, type, symbol, params).
    (Features with different names must not be merged silently.)
    """
    seen = set()
    out = []
    for f in features:
        key = (
            f.get("name"),
            f["type"],
            f.get("symbol"),
            tuple(sorted(f.get("params", {}).items())) if "params" in f else None,
        )
        if key not in seen:
            seen.add(key)
            out.append(f)
    return out


# ----------------------------------------------------------------------
# MAIN ENTRY
# ----------------------------------------------------------------------
def resolve_feature_config(
    *,
    primary_symbol: str,
    user_features: List[Dict[str, Any]],
    required_feature_types: set[str],
) -> List[Dict[str, Any]]:
    """
    Strategy-driven feature resolver (v4).
    - return final feature configs (with NAMES preserved)
    """

    normalized: List[Dict[str, Any]] = []

    for f in user_features:
        if "type" not in f:
            raise ValueError(f"Invalid feature spec (missing type): {f}")

        spec = dict(f)

        # Attach symbol if omitted
        spec.setdefault("symbol", primary_symbol)

        # Ensure params is a dict if present
        if "params" in spec and spec["params"] is not None:
            if not isinstance(spec["params"], dict):
                raise TypeError(f"Feature params must be dict: {spec}")
        else:
            spec["params"] = {}

        normalized.append(spec)

    final = _dedupe(normalized)

    feature_names = {f["name"] for f in final if "name" in f}
    feature_types = {f["type"] for f in final}


    return final

def check_missing_features(
    *,
    feature_configs: List[Dict[str, Any]],
    model,
    risk_manager=None,
    decision=None,
) -> None:
    """
    Post-resolution feature validation (v4).

    This function is called AFTER:
        - model / risk / decision objects are constructed
        - feature configs are resolved (names + types fixed)

    Responsibilities:
        - validate feature TYPE coverage (design-time)
        - inject resolved feature NAMES into consumers
        - validate final NAME-level dependencies

    It does NOT:
        - infer new features
        - mutate feature configs
    """

    feature_types = {f["type"] for f in feature_configs}
    feature_names = [f["name"] for f in feature_configs]

    # --- Model ---
    if model is not None:
        if hasattr(model, "validate_feature_types"):
            model.validate_feature_types(feature_types)
        if hasattr(model, "set_required_features"):
            model.set_required_features(feature_names)
        if hasattr(model, "validate_features"):
            model.validate_features(feature_names)

    # --- Risk rules ---
    if risk_manager is not None:
        for rule in getattr(risk_manager, "rules", []):
            if hasattr(rule, "validate_feature_types"):
                rule.validate_feature_types(feature_types)
            if hasattr(rule, "set_required_features"):
                rule.set_required_features(feature_names)
            if hasattr(rule, "validate_features"):
                rule.validate_features(feature_names)

    # --- Decision ---
    if decision is not None:
        if hasattr(decision, "validate_feature_types"):
            decision.validate_feature_types(feature_types)
        if hasattr(decision, "set_required_features"):
            decision.set_required_features(feature_names)
        if hasattr(decision, "validate_features"):
            decision.validate_features(feature_names)