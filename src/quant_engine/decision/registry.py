# decision/registry.py
from quant_engine.contracts.decision import DecisionProto

DECISION_REGISTRY: dict[str, type[DecisionProto]] = {}

def register_decision(name: str):
    def decorator(cls):
        DECISION_REGISTRY[name] = cls
        return cls
    return decorator


def build_decision(name: str, **kwargs) -> DecisionProto:
    if name not in DECISION_REGISTRY:
        raise ValueError(f"Decision '{name}' not found in registry.")
    return DECISION_REGISTRY[name](**kwargs)