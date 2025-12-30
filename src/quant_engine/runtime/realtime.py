from __future__ import annotations

import time
import asyncio
import threading
from quant_engine.runtime.driver import BaseDriver
from quant_engine.runtime.lifecycle import RuntimePhase
from quant_engine.runtime.modes import EngineSpec
from quant_engine.runtime.snapshot import EngineSnapshot
from quant_engine.strategy.engine import StrategyEngine
from collections.abc import AsyncIterator


class RealtimeDriver(BaseDriver):
    """
    Realtime trading driver (v4).

    Semantics:
      - Engine-time advances according to EngineSpec.advance().
      - Runtime loop is open-ended.
      - Ingestion is external to runtime (apps / wiring layer).
    """

    def __init__(
        self,
        *,
        engine: StrategyEngine,
        spec: EngineSpec,
        stop_event: threading.Event | None = None,
    ):
        super().__init__(engine=engine, spec=spec, stop_event=stop_event)

    # -------------------------------------------------
    # Time progression
    # -------------------------------------------------

    async def iter_timestamps(self) -> AsyncIterator[int]:
        """Yield engine-time timestamps (epoch ms int) in strictly increasing order.

        Realtime semantics:
          - Driver owns the strategy observation clock.
          - We pace steps to wall-clock using sleep.
          - If the process is delayed (e.g. long step), we do not busy-loop.
        """
        current_ts = int(self.spec.timestamp) if self.spec.timestamp is not None else int(time.time() * 1000)

        while True:
            yield current_ts

            next_ts = int(self.spec.advance(current_ts))

            # Pace to wall-clock.
            now = int(time.time() * 1000)
            sleep_ms = next_ts - now
            if sleep_ms > 0:
                await asyncio.sleep(sleep_ms / 1000.0)
            else:
                # We're late; yield control to let ingestion tasks run,
                # but do not spin.
                await asyncio.sleep(0)

            current_ts = next_ts
    
    async def run(self) -> None:
        anchor_ts = int(self.spec.timestamp) if self.spec.timestamp is not None else int(time.time() * 1000)
        try:
            self.guard.enter(RuntimePhase.PRELOAD)
            self.engine.preload_data(anchor_ts=anchor_ts)

            # -------- warmup --------
            self.guard.enter(RuntimePhase.WARMUP)
            self.engine.warmup_features(anchor_ts=anchor_ts)

            # -------- main loop --------
            async for ts in self.iter_timestamps():
                if self.stop_event.is_set():
                    break
                self.guard.enter(RuntimePhase.STEP)

                self.engine.align_to(ts)
                result = self.engine.step(ts=ts)

                # Yield to the event loop so background ingestion tasks can run
                # even if step() is CPU-heavy.
                await asyncio.sleep(0)

                if isinstance(result, EngineSnapshot):
                    self._snapshots.append(result)
                elif isinstance(result, dict):
                    self._snapshots.append(
                        EngineSnapshot(
                            timestamp=ts,
                            mode=self.spec.mode,
                            features=result.get("features", {}),
                            model_outputs=result.get("model_outputs", {}),
                            decision_score=result.get("decision_score"),
                            target_position=result.get("target_position"),
                            fills=result.get("fills", []),
                            market_data=result.get("market_data"),
                            portfolio=result.get(
                                "portfolio",
                                self.engine.portfolio.state(),
                            ),
                        )
                    )
        except asyncio.CancelledError:
            self._shutdown_components()
            raise
        except Exception as exc:
            self._handle_fatal(exc)

        # -------- finish --------
        self.guard.enter(RuntimePhase.FINISH)
