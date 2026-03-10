from __future__ import annotations

import time
import asyncio
import threading
from typing import cast
from quant_engine.runtime.driver import BaseDriver
from quant_engine.runtime.lifecycle import RuntimePhase
from quant_engine.runtime.modes import EngineSpec
from quant_engine.runtime.snapshot import EngineSnapshot
from collections.abc import AsyncIterator
from quant_engine.contracts.exchange_account import ExchangeAccountAdapter
from quant_engine.health.events import ActionKind
from quant_engine.health.manager import HealthManager
from quant_engine.health.session import market_is_closed
from quant_engine.data.contracts.snapshot import status_to_gap_type
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.exceptions.core import FatalError
from quant_engine.utils.asyncio import create_task_named, loop_lag_monitor, to_thread_limited
from quant_engine.utils.logger import log_error, log_info, log_warn
from quant_engine.utils.num import visible_end_ts

LOOP_LAG_INTERVAL_S = 1.0
LOOP_LAG_WARN_S = 0.2
_CATCHUP_MAX_ROUNDS = 3


def _align_realtime_step_ts(ts: int, interval_ms: int) -> int:
    current_ts = int(ts)
    step_ms = int(interval_ms)
    if step_ms <= 0:
        return current_ts
    remainder = current_ts % step_ms
    if remainder == 0:
        return current_ts
    return current_ts + (step_ms - remainder)


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
        health: HealthManager | None = None,
    ):
        super().__init__(engine=engine, spec=spec, stop_event=stop_event, health=health)

    def _reconcile_exchange_account_snapshot(self, snapshot: EngineSnapshot) -> EngineSnapshot:
        adapter = getattr(self.engine, "exchange_account_adapter", None)
        if adapter is None:
            return snapshot
        symbol = str(getattr(self.engine, "exchange_account_symbol", getattr(self.engine, "symbol", "")) or "").strip().upper()
        constraints = getattr(self.engine, "exchange_symbol_constraints", None)
        quote_asset = getattr(constraints, "quote_asset", None)
        if not symbol or not quote_asset:
            return snapshot

        account_state = cast(ExchangeAccountAdapter, adapter).get_account_state()
        self.engine.portfolio.sync_from_exchange(
            account_state,
            symbol=symbol,
            quote_asset=str(quote_asset),
        )
        reconciled = EngineSnapshot(
            timestamp=snapshot.timestamp,
            mode=snapshot.mode,
            features=snapshot.features,
            model_outputs=snapshot.model_outputs,
            decision_score=snapshot.decision_score,
            target_position=snapshot.target_position,
            fills=snapshot.fills,
            portfolio=self.engine.portfolio.state(),
            health=snapshot.health,
        )
        self.engine.engine_snapshot = reconciled

        quote_bal = account_state.balances.get(str(quote_asset))
        base_qty = float(account_state.positions.get(symbol, 0.0) or 0.0)
        log_info(
            self._logger,
            "runtime.exchange_account.reconciled",
            step_ts=snapshot.timestamp,
            symbol=symbol,
            exchange_quote_free=float(quote_bal.free) if quote_bal else 0.0,
            exchange_base_qty=base_qty,
        )
        return reconciled

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
        override = getattr(self, "_start_ts_override", None)
        current_ts = int(override) if override is not None else (
            int(self.spec.timestamp) if self.spec.timestamp is not None else int(time.time() * 1000)
        )
        interval_ms = int(self.spec.interval_ms)
        current_ts = _align_realtime_step_ts(current_ts, interval_ms)
        if override is not None:
            self._start_ts_override = None

        while True:
            now = int(time.time() * 1000)
            sleep_ms = current_ts - now
            if sleep_ms > 0:
                await asyncio.sleep(sleep_ms / 1000.0)
            else:
                # We're late; yield control to let ingestion tasks run,
                # but do not spin.
                await asyncio.sleep(0)

            yield current_ts

            current_ts = int(self.spec.advance(current_ts))
    
    async def run(self) -> None:
        anchor_ts = int(self.spec.timestamp) if self.spec.timestamp is not None else int(time.time() * 1000)
        self._install_loop_exception_handler()
        self._start_asyncio_heartbeat()
        self._background_tasks.append(
            create_task_named(
                loop_lag_monitor(
                    interval_s=LOOP_LAG_INTERVAL_S,
                    warn_after_s=LOOP_LAG_WARN_S,
                    logger=self._logger,
                    context={"driver": self.__class__.__name__},
                ),
                name="runtime.loop_lag_monitor",
                logger=self._logger,
                context={"driver": self.__class__.__name__},
                stop_event=self.stop_event,
            )
        )
        try:
            self.guard.enter(RuntimePhase.PRELOAD)
            # Intentional sync: keep engine single-threaded until thread-safe preload exists.
            self.engine.bootstrap(anchor_ts=anchor_ts)
            ohlcv_h = self.engine._get_primary_ohlcv_handler()
            if ohlcv_h is not None:
                interval_ms = getattr(ohlcv_h, "interval_ms", None)
                if isinstance(interval_ms, int) and interval_ms > 0:
                    required_ts = visible_end_ts(int(anchor_ts), int(interval_ms))
                    last_ts = ohlcv_h.last_timestamp()
                    # operational freshness gate: warn if cleaned bootstrap is stale; warmup/backfill policy handles recovery
                    if last_ts is None or int(last_ts) < int(required_ts):
                        log_warn(
                            self._logger,
                            "runtime.prewarm.cleaned_stale",
                            anchor_ts=int(anchor_ts),
                            required_ts=int(required_ts),
                            last_data_ts=int(last_ts) if last_ts is not None else None,
                        )

            # -------- warmup --------
            self.guard.enter(RuntimePhase.WARMUP)
            # Intentional sync: warmup mutates engine state; avoid cross-thread access.
            self.engine.warmup_features(anchor_ts=anchor_ts)

            # -------- catch-up (realtime/mock only) --------
            last_ts = int(anchor_ts)
            max_rounds = _CATCHUP_MAX_ROUNDS
            for round_idx in range(max_rounds):
                now_ts = int(self.spec.timestamp) if self.spec.timestamp is not None else int(time.time() * 1000)
                if now_ts <= last_ts:
                    break
                gaps = self._catch_up_once(from_ts=last_ts, to_ts=now_ts)
                last_ts = int(now_ts)
                if gaps and round_idx == max_rounds - 1:
                    self._raise_catchup_failure(
                        event="runtime.catchup.gaps_remaining",
                        gaps=gaps,
                        target_ts=int(now_ts),
                        message=f"Catch-up failed after {max_rounds} rounds; gaps remaining: {gaps}",
                    )

            if last_ts != int(anchor_ts):
                self._start_ts_override = int(last_ts)

            # -------- main loop --------
            async for ts in self.iter_timestamps():
                if self.stop_event.is_set():
                    break
                self.guard.enter(RuntimePhase.INGEST)
                # Scenario: external ingestion feeds handlers out-of-band; this boundary enforces phase ordering only.
                self.guard.enter(RuntimePhase.STEP)

                await to_thread_limited(
                    self.engine.align_to,
                    ts,
                    logger=self._logger,
                    context={"driver": self.__class__.__name__},
                    op="align_to",
                )
                ohlcv_h = self.engine._get_primary_ohlcv_handler()
                if ohlcv_h is not None:
                    interval_ms = getattr(ohlcv_h, "interval_ms", None)
                    if isinstance(interval_ms, int) and interval_ms > 0:
                        required_ts = visible_end_ts(int(ts), int(interval_ms))
                        last_ts = ohlcv_h.last_timestamp()
                        # realtime readiness gate: skip step if OHLCV is stale to avoid decisions on old data.
                        if last_ts is None or int(last_ts) < int(required_ts):
                            recovered, gaps = self._recover_mainloop_ohlcv_gap(
                                required_ts=int(required_ts),
                                last_data_ts=int(last_ts) if last_ts is not None else None,
                                interval_ms=int(interval_ms),
                            )
                            if recovered:
                                last_ts = ohlcv_h.last_timestamp()
                                if last_ts is not None and int(last_ts) >= int(required_ts):
                                    # Gap recovered in bounded retries; proceed with step.
                                    pass
                                else:
                                    recovered = False
                            else:
                                if gaps:
                                    self._raise_catchup_failure(
                                        event="runtime.mainloop.gaps_remaining",
                                        gaps=gaps,
                                        target_ts=int(required_ts),
                                        message=(
                                            f"mainloop catch-up failed after {_CATCHUP_MAX_ROUNDS} rounds; "
                                            f"gaps remaining: {gaps}"
                                        ),
                                        step_ts=int(ts),
                                        required_ts=int(required_ts),
                                    )
                            if not recovered:
                                log_warn(
                                    self._logger,
                                    "runtime.step.data_not_ready",
                                    step_ts=int(ts),
                                    required_ts=int(required_ts),
                                    last_data_ts=int(last_ts) if last_ts is not None else None,
                                )
                                if self._health is not None:
                                    self._health.report_step_skipped(int(ts))
                                    if not self._health.is_step_allowed():
                                        raise FatalError("health.halt: too many consecutive skips")
                                continue
                if self._health is not None:
                    staleness_actions = self._health.check_staleness(int(ts))
                    for staleness_action in staleness_actions:
                        if staleness_action.kind == ActionKind.HALT:
                            # Invariant: health HALT must terminate loop before executing a new strategy step.
                            raise FatalError(
                                f"health.halt: staleness escalation: {staleness_action.detail}"
                            )
                # Intentional sync: step must remain single-threaded to preserve engine invariants.
                result = self.engine.step(ts=ts)

                # Yield to the event loop so background ingestion tasks can run
                # even if step() is CPU-heavy.
                await asyncio.sleep(0)

                if not isinstance(result, EngineSnapshot):
                    raise TypeError(f"engine.step() must return EngineSnapshot, got {type(result).__name__}")
                try:
                    result = await to_thread_limited(
                        self._reconcile_exchange_account_snapshot,
                        result,
                        logger=self._logger,
                        context={"driver": self.__class__.__name__},
                        op="exchange_account_reconcile",
                    )
                except Exception as exc:
                    log_warn(
                        self._logger,
                        "runtime.exchange_account.reconcile_failed",
                        err_type=type(exc).__name__,
                        err=str(exc),
                    )
                self._snapshots.append(result)
        except asyncio.CancelledError:
            self._shutdown_components()
            raise
        except Exception as exc:
            self._handle_fatal(exc)
        finally:
            await self._cancel_background_tasks()

        # -------- finish --------
        self.guard.enter(RuntimePhase.FINISH)

    def _classify_catchup_failure(self, *, gaps: list[str], target_ts: int) -> tuple[str, dict[str, object]]:
        hard_domains: set[str] = {"ohlcv"}
        cfg = getattr(self._health, "_cfg", None)
        cfg_domains = getattr(cfg, "domains", None)
        if isinstance(cfg_domains, dict):
            for domain, policy in cfg_domains.items():
                if getattr(policy, "criticality", None) == "hard":
                    hard_domains.add(str(domain))
        hard_gaps = [g for g in gaps if str(g).split(":", 1)[0] in hard_domains]
        if not hard_gaps:
            return "HARD_DATA_GAP_FATAL", {"hard_gap": False, "hard_domains": sorted(hard_domains)}

        ohlcv_h = self.engine._get_primary_ohlcv_handler()
        market = getattr(ohlcv_h, "market", None) if ohlcv_h is not None else None
        status = getattr(market, "status", None)
        gap_type = getattr(market, "gap_type", None)
        calendar = getattr(market, "calendar", None)
        status_gap = status_to_gap_type(status)
        market_closed = False
        if isinstance(calendar, str) and calendar.strip():
            market_closed = market_is_closed(int(target_ts), {"calendar": calendar})
        maintenance_like = str(gap_type) in {"expected_closed", "halt"} or str(status_gap) in {"expected_closed", "halt"} or market_closed
        if maintenance_like:
            return "MAINTENANCE_STOP", {
                "hard_gap": True,
                "hard_gaps": hard_gaps,
                "status": status,
                "gap_type": gap_type,
                "calendar": calendar,
                "market_closed": market_closed,
            }
        return "HARD_DATA_GAP_FATAL", {
            "hard_gap": True,
            "hard_gaps": hard_gaps,
            "status": status,
            "gap_type": gap_type,
            "calendar": calendar,
            "market_closed": market_closed,
        }

    def _raise_catchup_failure(
        self,
        *,
        event: str,
        gaps: list[str],
        target_ts: int,
        message: str,
        step_ts: int | None = None,
        required_ts: int | None = None,
    ) -> None:
        shutdown_code, evidence = self._classify_catchup_failure(
            gaps=gaps,
            target_ts=int(target_ts),
        )
        log_error(
            self._logger,
            event,
            driver=self.__class__.__name__,
            missing=gaps,
            target_ts=int(target_ts),
            step_ts=int(step_ts) if step_ts is not None else None,
            required_ts=int(required_ts) if required_ts is not None else None,
            shutdown_code=shutdown_code,
            evidence=evidence,
        )
        raise FatalError(f"{shutdown_code}: {message}")

    def _recover_mainloop_ohlcv_gap(
        self,
        *,
        required_ts: int,
        last_data_ts: int | None,
        interval_ms: int,
    ) -> tuple[bool, list[str]]:
        max_rounds = _CATCHUP_MAX_ROUNDS
        from_ts = int(last_data_ts) if last_data_ts is not None else int(required_ts) - int(interval_ms)
        if from_ts < 0:
            from_ts = 0
        final_gaps: list[str] = []
        for _ in range(max_rounds):
            gaps = self._catch_up_once(from_ts=int(from_ts), to_ts=int(required_ts))
            final_gaps = gaps
            if not gaps:
                # No identified catch-up domain means no deterministic recovery path here.
                return False, []
            ohlcv_h = self.engine._get_primary_ohlcv_handler()
            post_last = ohlcv_h.last_timestamp() if ohlcv_h is not None else None
            if post_last is not None and int(post_last) >= int(required_ts):
                return True, []
            if post_last is not None:
                from_ts = int(post_last)
            else:
                from_ts = int(required_ts) - int(interval_ms)
        return False, final_gaps
