from __future__ import annotations

import gc
import os
from typing import Any

from quant_engine.utils.logger import log_debug


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip() or str(default))
    except Exception:
        return int(default)


class PeriodicMemoryTrim:
    """Best-effort periodic memory pressure relief for long-running ingestion loops."""

    def __init__(self, *, component: str):
        self._component = str(component)
        self._every = max(0, _env_int("INGEST_MEMORY_TRIM_EVERY", 200))
        self._gc_gen = max(0, min(2, _env_int("INGEST_MEMORY_TRIM_GEN", 2)))
        self._step = 0

    def maybe_run(self, *, logger: Any, **context: Any) -> None:
        if self._every <= 0:
            return
        self._step += 1
        if self._step % self._every != 0:
            return
        collected = gc.collect(self._gc_gen)
        arrow_released = False
        try:
            import pyarrow as pa

            pool = pa.default_memory_pool()
            if hasattr(pool, "release_unused"):
                pool.release_unused()
                arrow_released = True
        except Exception:
            arrow_released = False
        log_debug(
            logger,
            "ingestion.memory_trim",
            component=self._component,
            step=self._step,
            gc_gen=self._gc_gen,
            gc_collected=int(collected),
            arrow_release_unused=bool(arrow_released),
            **context,
        )
