# backtest/engine.py

import time

from quant_engine.strategy.engine import StrategyEngine
from quant_engine.strategy.engine import EngineMode

from quant_engine.utils.logger import get_logger
from quant_engine.runtime.log_router import attach_artifact_handlers


class BacktestEngine:
    """
    Deterministic Backtest Driver.

    Responsibilities:
    - Own backtest time range
    - Load historical data
    - Warm up strategy state
    - Drive StrategyEngine.step() forward in time

    StrategyEngine owns:
    - data handlers
    - features
    - models / risk / decision
    """

    def __init__(
        self,
        *,
        engine: StrategyEngine,
        start_ts: float,
        end_ts: float,
        warmup_steps: int = 0,
        run_id: str | None = None,
    ):
        if engine.mode != EngineMode.BACKTEST:
            raise ValueError("BacktestEngine requires EngineMode.BACKTEST")

        self.engine = engine
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.warmup_steps = warmup_steps
        self.run_id = run_id or f"backtest_{int(time.time())}"

        self._logger = get_logger(__name__)
        attach_artifact_handlers(self._logger, run_id=self.run_id)

    # -------------------------------------------------
    # Lifecycle
    # -------------------------------------------------
    def run(self) -> None:
        """
        Execute the backtest deterministically.
        """

        self._logger.info(
            "Backtest started",
            extra={"context": {
                "start_ts": self.start_ts,
                "end_ts": self.end_ts,
                "warmup_steps": self.warmup_steps,
            }},
        )

        # 1) Load historical data
        self.engine.load_history(start_ts=self.start_ts, end_ts=self.end_ts)

        # 2) Warm up strategy state
        self.engine.warmup(anchor_ts=self.start_ts, warmup_steps=self.warmup_steps)

        # 3) Run main backtest loop
        prev_ts: float | None = None
        steps = 0
        while True:
            snapshot = self.engine.step()

            if "timestamp" not in snapshot:
                raise RuntimeError(
                    "StrategyEngine.step() must return a snapshot "
                    "with a 'timestamp' field in BACKTEST mode"
                )

            try:
                ts = float(snapshot["timestamp"])
            except Exception as e:
                raise RuntimeError(f"Invalid snapshot timestamp: {snapshot.get('timestamp')!r}") from e

            # Guard against non-advancing clocks (infinite loops)
            if prev_ts is not None and ts <= prev_ts:
                raise RuntimeError(
                    f"Backtest clock did not advance (prev_ts={prev_ts}, ts={ts}). "
                    "Check that the primary OHLCV handler advances its cursor per step() "
                    "(e.g., get_snapshot() should reflect the current cursor and advance deterministically)."
                )

            prev_ts = ts
            steps += 1

            if ts >= self.end_ts:
                break

        self._logger.info("Backtest completed")
