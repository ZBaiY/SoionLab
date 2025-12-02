##  Profiling / latency measurement utilities
from contextlib import contextmanager
import time

@contextmanager
def timer(name, log=None):
    t0 = time.time()
    yield
    t1 = time.time()
    if log:
        log.debug(f"[TIMER] {name}: {t1 - t0:.6f}s")