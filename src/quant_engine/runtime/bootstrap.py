from __future__ import annotations

from quant_engine.strategy.engine import StrategyEngine


def run_bootstrap(
    *,
    engine: StrategyEngine,
    anchor_ts: int | None = None,
) -> None:
    """
    Execute the data bootstrap / preload phase.
    NOTE:
      - This is a thin wrapper around StrategyEngine.preload_data().
      - It intentionally contains no logic of its own.
      - anchor_ts is epoch milliseconds (int) when provided.
    """
    engine.preload_data(anchor_ts=anchor_ts)
