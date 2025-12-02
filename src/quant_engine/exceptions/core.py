class QuantEngineError(Exception):
    pass

class ConfigError(QuantEngineError):
    pass

class DataError(QuantEngineError):
    pass

class ExecutionError(QuantEngineError):
    pass