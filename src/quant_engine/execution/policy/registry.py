POLICY_REGISTRY = {}

def register_policy(name: str):
    def decorator(cls):
        POLICY_REGISTRY[name] = cls
        return cls
    return decorator

def build_policy(name: str, symbol: str, **kwargs):
    return POLICY_REGISTRY[name](symbol=symbol, **kwargs)

from quant_engine.execution.policy.immediate import *
from quant_engine.execution.policy.maker_first import * # type: ignore
from quant_engine.execution.policy.twap import * # type: ignore