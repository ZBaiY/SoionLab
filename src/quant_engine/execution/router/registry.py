ROUTER_REGISTRY = {}

def register_router(name: str):
    def decorator(cls):
        ROUTER_REGISTRY[name] = cls
        return cls
    return decorator

def build_router(name: str, **kwargs):
    return ROUTER_REGISTRY[name](**kwargs)