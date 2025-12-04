# models/registry.py
from quant_engine.contracts.model import ModelProto

MODEL_REGISTRY: dict[str, type[ModelProto]] = {}

def register_model(name: str):
    """Decorator: @register_model("OU_MODEL")"""
    def decorator(cls):
        MODEL_REGISTRY[name] = cls
        return cls
    return decorator


def build_model(name: str, **kwargs) -> ModelProto:
    if name not in MODEL_REGISTRY:
        raise ValueError(f"Model '{name}' not found in registry.")
    return MODEL_REGISTRY[name](**kwargs)