from __future__ import annotations

import threading
import time

from quant_engine.health.config import DomainPolicyCfg, FaultPolicyCfg
from quant_engine.health.events import Action, ActionKind, ExecutionPermit, FaultEvent, FaultKind
from quant_engine.health.policy import FaultPolicy
from quant_engine.health.session import market_is_closed
from quant_engine.health.snapshot import DomainHealthState, HealthSnapshot, GlobalSafetyMode
from quant_engine.health.state import DomainHealth, GlobalSafety, make_health_key


class HealthManager:
    def __init__(
        self,
        cfg: FaultPolicyCfg,
        logger,
        *,
        calendar_cfg: dict[str, str] | None = None,
    ) -> None:
        self._cfg = cfg
        self._logger = logger
        self._calendar_cfg = dict(calendar_cfg or {})
        self._lock = threading.Lock()
        self._domains: dict[tuple[str, str], DomainHealth] = {}
        self._global = GlobalSafety()
        self._policy = FaultPolicy()

    def report(self, event: FaultEvent) -> Action:
        with self._lock:
            # Role: key collapses event routing to a deterministic per-domain/per-symbol health state bucket.
            key = make_health_key(event)
            if key not in self._domains:
                self._domains[key] = DomainHealth(*key)
            action = self._policy.evaluate(event, self._domains, self._global, self._cfg)
            self._apply_time_escalation(int(event.ts))
            return action

    def report_tick(self, domain: str, symbol: str, ts: int) -> None:
        now_ms = int(ts)
        with self._lock:
            key = (str(domain or "__engine__"), str(symbol or "__all__"))
            state = self._domains.get(key)
            if state is None:
                state = DomainHealth(*key)
                self._domains[key] = state
            state.last_tick_ts = now_ms
            domain_cfg = self._domain_cfg_for(key[0])
            if state.state in (DomainHealthState.DEGRADED, DomainHealthState.RECOVERING):
                state.apply_tick_ok(domain_cfg)
            self._reconcile_global_mode(now_ms)

    def report_step_ok(self, ts: int) -> None:
        now_ms = int(ts)
        with self._lock:
            self._global.record_step_ok(now_ms)
            self._reconcile_global_mode(now_ms)

    def report_step_skipped(self, ts: int) -> None:
        now_ms = int(ts)
        with self._lock:
            self._global.record_step_skipped(now_ms)
            if self._global.consecutive_skips >= max(1, int(self._cfg.hard_fail_threshold)):
                self._global.set_mode(GlobalSafetyMode.HALT, now_ms)
            elif self._global.consecutive_skips >= max(1, int(self._cfg.skip_threshold)):
                self._global.set_mode(GlobalSafetyMode.SAFE_HOLD, now_ms)
            self._apply_time_escalation(now_ms)

    def check_staleness(self, ts: int) -> list[Action]:
        now_ms = int(ts)
        actions: list[Action] = []
        with self._lock:
            for (domain, symbol), state in list(self._domains.items()):
                if domain == "__engine__":
                    continue
                domain_cfg = self._domain_cfg_for(domain)
                if state.check_probe_timeout(now_ms, domain_cfg):
                    timeout_action = self._policy.evaluate(
                        FaultEvent(
                            ts=now_ms,
                            source="health.probe_timeout",
                            kind=FaultKind.SOURCE_EXHAUSTED,
                            domain=domain,
                            symbol=symbol,
                            context={"probe_timeout": True},
                        ),
                        self._domains,
                        self._global,
                        self._cfg,
                    )
                    actions.append(timeout_action)

                if int(domain_cfg.staleness_ms) <= 0:
                    continue
                if domain_cfg.staleness_session_aware:
                    cal_name = self._calendar_cfg.get(domain)
                    if market_is_closed(now_ms, {"calendar": cal_name} if cal_name else None):
                        # Scenario: session-aware domains suppress stale escalation while market calendar is closed.
                        continue
                stale = False
                staleness_ms: int | None = None
                if state.last_tick_ts is None:
                    stale = True
                else:
                    staleness_ms = max(0, now_ms - int(state.last_tick_ts))
                    stale = staleness_ms > int(domain_cfg.staleness_ms)
                if not stale:
                    continue
                action = self._policy.evaluate(
                    FaultEvent(
                        ts=now_ms,
                        source="health.check_staleness",
                        kind=FaultKind.TICK_STALE,
                        domain=domain,
                        symbol=symbol,
                        context={"staleness_ms": staleness_ms},
                    ),
                    self._domains,
                    self._global,
                    self._cfg,
                )
                actions.append(action)
            self._apply_time_escalation(now_ms)
        return actions

    def report_recovery(self, domain: str, symbol: str) -> Action:
        now_ms = int(time.time() * 1000)
        with self._lock:
            key = (str(domain or "__engine__"), str(symbol or "__all__"))
            state = self._domains.get(key)
            if state is None:
                state = DomainHealth(*key)
                self._domains[key] = state
            domain_cfg = self._domain_cfg_for(key[0])
            state.apply_restart(domain_cfg, now_ms=now_ms)
            if state.state == DomainHealthState.RECOVERING:
                return Action(
                    kind=ActionKind.CONTINUE,
                    domain=key[0],
                    symbol=key[1],
                    detail="recovery probe started",
                    execution_permit=self._permit_for_mode(self._global.mode),
                )
            self._reconcile_global_mode(now_ms)
            return self._policy.evaluate(
                FaultEvent(
                    ts=now_ms,
                    source="health.report_recovery",
                    kind=FaultKind.SOURCE_EXHAUSTED,
                    domain=key[0],
                    symbol=key[1],
                    context={"max_restarts_exceeded": True},
                ),
                self._domains,
                self._global,
                self._cfg,
            )

    def snapshot(self) -> HealthSnapshot:
        now_ms = int(time.time() * 1000)
        with self._lock:
            domains = {
                f"{domain}:{symbol}": state.to_summary(now_ms)
                for (domain, symbol), state in self._domains.items()
            }
            return HealthSnapshot(
                ts=now_ms,
                global_mode=self._global.mode,
                domains=domains,
                consecutive_skips=int(self._global.consecutive_skips),
                consecutive_step_failures=int(self._global.consecutive_step_failures),
                last_step_ts=self._global.last_step_ts,
                execution_permit=self._permit_for_mode(self._global.mode),
            )

    def global_mode(self) -> GlobalSafetyMode:
        with self._lock:
            return self._global.mode

    def is_step_allowed(self) -> bool:
        with self._lock:
            return self._global.mode != GlobalSafetyMode.HALT

    def execution_permit(self) -> ExecutionPermit:
        with self._lock:
            return self._permit_for_mode(self._global.mode)

    def restart_delay_ms(self, domain: str, symbol: str) -> int:
        with self._lock:
            key = (str(domain or "__engine__"), str(symbol or "__all__"))
            state = self._domains.get(key)
            if state is None:
                domain_cfg = self._domain_cfg_for(key[0])
                return max(0, int(domain_cfg.restart_delay_base_ms))
            if int(state.restart_delay_ms) > 0:
                return int(state.restart_delay_ms)
            domain_cfg = self._domain_cfg_for(key[0])
            return max(0, int(domain_cfg.restart_delay_base_ms))

    def _apply_time_escalation(self, now_ms: int) -> None:
        if self._global.mode == GlobalSafetyMode.SAFE_HOLD and self._global.safe_hold_since is not None:
            if int(now_ms) - int(self._global.safe_hold_since) >= max(0, int(self._cfg.flatten_timeout_ms)):
                self._global.set_mode(GlobalSafetyMode.SAFE_FLATTEN, now_ms)
        if self._global.mode == GlobalSafetyMode.SAFE_FLATTEN and self._global.safe_flatten_since is not None:
            if int(now_ms) - int(self._global.safe_flatten_since) >= max(0, int(self._cfg.halt_timeout_ms)):
                self._global.set_mode(GlobalSafetyMode.HALT, now_ms)

    def _reconcile_global_mode(self, now_ms: int) -> None:
        if self._global.mode == GlobalSafetyMode.HALT:
            return
        hard_circuit = False
        hard_degraded = False
        soft_circuit_count = 0
        for (domain, _symbol), state in self._domains.items():
            if domain == "__engine__":
                continue
            domain_cfg = self._domain_cfg_for(domain)
            if state.state == DomainHealthState.CIRCUIT_OPEN:
                if domain_cfg.criticality == "hard":
                    hard_circuit = True
                else:
                    soft_circuit_count += 1
            elif state.state == DomainHealthState.DEGRADED and domain_cfg.criticality == "hard":
                hard_degraded = True
        hold_needed = hard_circuit
        degraded_needed = hard_degraded or soft_circuit_count >= max(1, int(self._cfg.soft_circuit_escalation_count))
        if self._global.consecutive_step_failures >= max(1, int(self._cfg.step_fail_threshold)):
            hold_needed = True
        if self._global.consecutive_skips >= max(1, int(self._cfg.skip_threshold)):
            hold_needed = True

        if self._global.mode == GlobalSafetyMode.SAFE_FLATTEN:
            if hold_needed:
                return
            self._global.set_mode(GlobalSafetyMode.SAFE_HOLD, now_ms)
            return
        if self._global.mode == GlobalSafetyMode.SAFE_HOLD:
            if hold_needed:
                return
            if degraded_needed:
                self._global.set_mode(GlobalSafetyMode.DEGRADED, now_ms)
                return
            self._global.set_mode(GlobalSafetyMode.RUNNING, now_ms)
            return
        if hold_needed:
            self._global.set_mode(GlobalSafetyMode.SAFE_HOLD, now_ms)
            return
        if degraded_needed:
            self._global.set_mode(GlobalSafetyMode.DEGRADED, now_ms)
            return
        self._global.set_mode(GlobalSafetyMode.RUNNING, now_ms)

    def _domain_cfg_for(self, domain: str) -> DomainPolicyCfg:
        found = self._cfg.domains.get(domain)
        if found is not None:
            return found
        return DomainPolicyCfg(
            criticality=self._cfg.unknown_domain_criticality,
            degrade_threshold=1,
            circuit_threshold=1,
            max_restarts=0,
            staleness_ms=0,
            staleness_session_aware=False,
            restart_delay_base_ms=5_000,
        )

    @staticmethod
    def _permit_for_mode(mode: GlobalSafetyMode) -> ExecutionPermit:
        if mode == GlobalSafetyMode.HALT:
            return ExecutionPermit.BLOCK
        if mode in (GlobalSafetyMode.SAFE_HOLD, GlobalSafetyMode.SAFE_FLATTEN):
            # Invariant: both hold and flatten operate under reduce-only execution permissions.
            return ExecutionPermit.REDUCE_ONLY
        return ExecutionPermit.FULL
