POLICY_REGISTRY = {}

def register_policy(name: str):
    def decorator(cls):
        POLICY_REGISTRY[name] = cls
        return cls
    return decorator

def build_policy(name: str, **kwargs):
    return POLICY_REGISTRY[name](**kwargs)