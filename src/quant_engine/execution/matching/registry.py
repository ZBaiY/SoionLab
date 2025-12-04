MATCHING_REGISTRY = {}

def register_matching(name: str):
    def decorator(cls):
        MATCHING_REGISTRY[name] = cls
        return cls
    return decorator

def build_matching(name: str, **kwargs):
    return MATCHING_REGISTRY[name](**kwargs)