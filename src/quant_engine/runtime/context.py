from __future__ import annotations

from dataclasses import dataclass

from quant_engine.runtime.modes import EngineMode


@dataclass(frozen=True)
class RuntimeContext:
    """
    Immutable runtime context.

    Semantics:
      - Represents the current engine-time execution context.
      - Owned by runtime / driver.
      - May be passed to lower layers for read-only access.

    Non-responsibilities:
      - Does NOT own data handlers or caches.
      - Does NOT advance time.
      - Does NOT encode strategy logic.
    """

    timestamp: float
    mode: EngineMode
