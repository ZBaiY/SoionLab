# risk/registry.py
from quant_engine.contracts.risk import RiskProto


RISK_REGISTRY: dict[str, type[RiskProto]] = {}

def register_risk(name: str):
    def decorator(cls):
        RISK_REGISTRY[name] = cls
        return cls
    return decorator


def build_risk(name: str, **kwargs) -> RiskProto:
    if name not in RISK_REGISTRY:
        raise ValueError(f"Risk rule '{name}' not found.")
    return RISK_REGISTRY[name](**kwargs)