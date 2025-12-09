# features/registry.py
from quant_engine.contracts.feature import FeatureChannel

### Feature registry and builder, this is because we have many features.

# Global registry
FEATURE_REGISTRY: dict[str, type] = {}

# ----------------------------------------------------------------------
# IMPORTANT:
# We do NOT import concrete feature classes here.
# Every feature module must self-register using @register_feature.
# This keeps registry lightweight and avoids circular imports.
# ----------------------------------------------------------------------

def register_feature(name: str):
    """
    Decorator: @register_feature("RSI")
    Automatically registers class into FEATURE_REGISTRY.
    """
    def decorator(cls):
        FEATURE_REGISTRY[name] = cls
        return cls
    return decorator


def build_feature(name: str, symbol=None, **params) -> FeatureChannel:
    """Instantiate a feature class by name (multi‑symbol ready)."""
    if name not in FEATURE_REGISTRY:
        raise ValueError(f"Feature '{name}' not found in registry.")
    
    cls = FEATURE_REGISTRY[name]

    return cls(symbol=symbol, **params)
