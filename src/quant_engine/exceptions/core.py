class QuantEngineError(Exception):
    pass

class ConfigError(QuantEngineError):
    pass

class DataError(QuantEngineError):
    pass

class ExecutionError(QuantEngineError):
    pass


class RecoverableError(QuantEngineError):
    """Transient or retryable failure; should be handled in ingestion/source layers."""


class FatalError(QuantEngineError):
    """Non-recoverable failure requiring supervised shutdown."""
