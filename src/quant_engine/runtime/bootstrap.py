from __future__ import annotations

from quant_engine.strategy.engine import StrategyEngine


def run_bootstrap(
    *,
    engine: StrategyEngine,
    anchor_ts: float | None = None,
) -> None:
    """
    Execute the data bootstrap / preload phase.
    NOTE:
      - This is a thin wrapper around StrategyEngine.preload_data().
      - It intentionally contains no logic of its own.
    """
    engine.preload_data(anchor_ts=anchor_ts)
