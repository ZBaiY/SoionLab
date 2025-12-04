SLIPPAGE_REGISTRY = {}

def register_slippage(name: str):
    def decorator(cls):
        SLIPPAGE_REGISTRY[name] = cls
        return cls
    return decorator

def build_slippage(name: str, **kwargs):
    return SLIPPAGE_REGISTRY[name](**kwargs)