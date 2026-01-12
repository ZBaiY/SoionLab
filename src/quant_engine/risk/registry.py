# risk/registry.py
from quant_engine.contracts.risk import RiskBase

RISK_REGISTRY: dict[str, type[RiskBase]] = {}

def register_risk(name: str):
    def decorator(cls):
        RISK_REGISTRY[name] = cls
        return cls
    return decorator


def build_risk(name: str, symbol: str, **kwargs) -> RiskBase:
    if name not in RISK_REGISTRY:
        raise ValueError(f"Risk rule '{name}' not found.")
    return RISK_REGISTRY[name](symbol=symbol, **kwargs)

from .rules_atr import *
from .rules_exposure import *
from .rules_sentiment import *
from .rules_sl import *
from .rules_tp import *
from .rule_full import *
from .rules_constraints import *
