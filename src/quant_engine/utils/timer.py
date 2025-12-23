import time
from contextlib import contextmanager
from .logger import get_logger, log_debug


@contextmanager
def timed_block(name: str, level="debug"):
    """Profile execution time of a code block."""
    logger = get_logger()

    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = (time.perf_counter() - start) * 1000  # ms
        log_debug(logger, f"[TIMER] {name}", elapsed_ms=round(elapsed, 3))

def advance_ts(ts: float, interval: str) -> float:
    """Advance a timestamp by a given interval string (e.g. '1m', '5m', '1h')."""
    unit = interval[-1]
    amount = int(interval[:-1])

    if unit == 'm':
        return ts + amount * 60
    elif unit == 'h':
        return ts + amount * 3600
    elif unit == 'd':
        return ts + amount * 86400
    else:
        raise ValueError(f"Unsupported interval unit: {unit!r}")


"""
Example usage:
from quant_engine.utils.timer import timed_block
from quant_engine.utils.logger import get_logger, log_info

logger = get_logger()

with timed_block("feature_extraction"):
    features = extractor.compute(data)

log_info(logger, "Features computed", rows=len(features))
"""