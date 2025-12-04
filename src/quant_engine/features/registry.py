# features/registry.py
from quant_engine.contracts.feature import FeatureChannel

### Feature registry and builder, this is because we have many features.

# Global registry
FEATURE_REGISTRY: dict[str, type[FeatureChannel]] = {}

def register_feature(name: str):
    """
    Decorator: @register_feature("RSI")
    Automatically registers class into FEATURE_REGISTRY.
    """
    def decorator(cls):
        FEATURE_REGISTRY[name] = cls
        return cls
    return decorator


def build_feature(name: str, **kwargs) -> FeatureChannel:
    """Instantiate a feature class by name."""
    if name not in FEATURE_REGISTRY:
        raise ValueError(f"Feature '{name}' not found in registry.")
    return FEATURE_REGISTRY[name](**kwargs)