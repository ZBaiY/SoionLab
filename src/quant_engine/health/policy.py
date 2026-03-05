from __future__ import annotations

from typing import Literal

from quant_engine.health.config import DomainPolicyCfg, FaultPolicyCfg
from quant_engine.health.events import (
    Action,
    ActionKind,
    ExecutionPermit,
    FaultEvent,
    FaultKind,
)
from quant_engine.health.snapshot import DomainHealthState, GlobalSafetyMode
from quant_engine.health.state import DomainHealth, GlobalSafety, make_health_key


Severity = Literal["transient", "degraded", "fatal"]

_GUARDRAIL_KINDS = {
    FaultKind.MONOTONICITY_VIOLATION,
    FaultKind.LOOKAHEAD_VIOLATION,
    FaultKind.SCHEMA_DRIFT,
    FaultKind.STEP_ORDER_VIOLATION,
}

_SOURCE_FAILURE_KINDS = {
    FaultKind.SOURCE_EXCEPTION,
    FaultKind.SOURCE_EXHAUSTED,
}

_SEVERITY_RANK: dict[Severity, int] = {
    "transient": 0,
    "degraded": 1,
    "fatal": 2,
}


class FaultPolicy:
    def evaluate(
        self,
        event: FaultEvent,
        domain_states: dict[tuple[str, str], DomainHealth],
        global_state: GlobalSafety,
        cfg: FaultPolicyCfg,
    ) -> Action:
        now_ms = int(event.ts)

        # 1) Guardrails always halt.
        if event.kind in _GUARDRAIL_KINDS:
            global_state.set_mode(GlobalSafetyMode.HALT, now_ms)
            return Action(
                kind=ActionKind.HALT,
                detail=f"guardrail violation: {event.kind.value}",
                execution_permit=ExecutionPermit.BLOCK,
            )

        key = make_health_key(event)
        domain_health = domain_states.get(key)
        if domain_health is None:
            domain_health = DomainHealth(*key)
            domain_states[key] = domain_health
        domain_cfg = self._domain_cfg_for(cfg, key[0])

        # 2) Severity inference with hint escalation only.
        inferred = self._infer_severity(event, domain_cfg, domain_health)
        effective = self._apply_hint(inferred, event.severity_hint)

        # 3) Domain-level FSM.
        if event.domain is not None:
            domain_health.apply_fault(domain_cfg, now_ms=now_ms)

        # Writer failures are explicitly log-only.
        if event.kind == FaultKind.WRITER_EXCEPTION:
            return Action(
                kind=ActionKind.LOG_ONLY,
                domain=event.domain,
                symbol=event.symbol,
                detail="writer exception",
                execution_permit=self._permit_for_mode(global_state.mode),
            )

        # 4) Execution uncertainty -> immediate SAFE_HOLD reduce-only.
        if event.kind == FaultKind.EXECUTION_EXCEPTION and bool((event.context or {}).get("uncertain")):
            global_state.set_mode(GlobalSafetyMode.SAFE_HOLD, now_ms)
            return Action(
                kind=ActionKind.FORCE_HOLD,
                domain=event.domain,
                symbol=event.symbol,
                detail="execution uncertainty",
                execution_permit=ExecutionPermit.REDUCE_ONLY,
            )

        # 5) Engine-level faults.
        if event.domain is None:
            return self._handle_engine_fault(event, global_state, cfg, now_ms)

        # Additional severity-based global shaping.
        if effective == "fatal":
            global_state.set_mode(GlobalSafetyMode.SAFE_HOLD, now_ms)
        elif effective == "degraded" and global_state.mode == GlobalSafetyMode.RUNNING:
            global_state.set_mode(GlobalSafetyMode.DEGRADED, now_ms)

        # 6) Domain -> global escalation.
        self._escalate_from_domains(domain_states, global_state, cfg, now_ms)

        if (
            event.kind in _SOURCE_FAILURE_KINDS
            and domain_health.state == DomainHealthState.CIRCUIT_OPEN
            and domain_health.recovery_attempts < max(0, int(domain_cfg.max_restarts))
        ):
            return Action(
                kind=ActionKind.RESTART_SOURCE,
                domain=event.domain,
                symbol=event.symbol,
                detail="domain circuit open; schedule restart",
                execution_permit=self._permit_for_mode(global_state.mode),
            )

        # 7) Time-based escalation.
        self._apply_time_escalation(global_state, cfg, now_ms)

        # 8) Mode -> Action.
        return self._action_for_mode(global_state.mode)

    def _handle_engine_fault(
        self,
        event: FaultEvent,
        global_state: GlobalSafety,
        cfg: FaultPolicyCfg,
        now_ms: int,
    ) -> Action:
        global_state.record_step_failure(now_ms)
        if global_state.consecutive_step_failures >= max(1, int(cfg.hard_fail_threshold)):
            global_state.set_mode(GlobalSafetyMode.HALT, now_ms)
            return Action(
                kind=ActionKind.HALT,
                detail=f"engine failures threshold reached: {event.kind.value}",
                execution_permit=ExecutionPermit.BLOCK,
            )
        if global_state.consecutive_step_failures >= max(1, int(cfg.step_fail_threshold)):
            global_state.set_mode(GlobalSafetyMode.SAFE_HOLD, now_ms)
            return Action(
                kind=ActionKind.FORCE_HOLD,
                detail=f"engine degraded: {event.kind.value}",
                execution_permit=ExecutionPermit.REDUCE_ONLY,
            )
        return Action(
            kind=ActionKind.SKIP_STEP,
            detail=f"engine step skipped due to {event.kind.value}",
            execution_permit=self._permit_for_mode(global_state.mode),
        )

    def _escalate_from_domains(
        self,
        domain_states: dict[tuple[str, str], DomainHealth],
        global_state: GlobalSafety,
        cfg: FaultPolicyCfg,
        now_ms: int,
    ) -> None:
        hard_circuit_open = False
        hard_degraded = False
        soft_circuit_open_count = 0
        for (domain, _symbol), state in domain_states.items():
            if domain == "__engine__":
                continue
            domain_cfg = self._domain_cfg_for(cfg, domain)
            if state.state == DomainHealthState.CIRCUIT_OPEN:
                if domain_cfg.criticality == "hard":
                    hard_circuit_open = True
                else:
                    soft_circuit_open_count += 1
            elif state.state == DomainHealthState.DEGRADED and domain_cfg.criticality == "hard":
                hard_degraded = True

        if hard_circuit_open:
            global_state.set_mode(GlobalSafetyMode.SAFE_HOLD, now_ms)
            return
        if hard_degraded:
            if global_state.mode == GlobalSafetyMode.RUNNING:
                global_state.set_mode(GlobalSafetyMode.DEGRADED, now_ms)
            return
        if soft_circuit_open_count >= max(1, int(cfg.soft_circuit_escalation_count)):
            if global_state.mode == GlobalSafetyMode.RUNNING:
                global_state.set_mode(GlobalSafetyMode.DEGRADED, now_ms)

    def _apply_time_escalation(
        self,
        global_state: GlobalSafety,
        cfg: FaultPolicyCfg,
        now_ms: int,
    ) -> None:
        if global_state.mode == GlobalSafetyMode.SAFE_HOLD and global_state.safe_hold_since is not None:
            if int(now_ms) - int(global_state.safe_hold_since) >= max(0, int(cfg.flatten_timeout_ms)):
                global_state.set_mode(GlobalSafetyMode.SAFE_FLATTEN, now_ms)
        if global_state.mode == GlobalSafetyMode.SAFE_FLATTEN and global_state.safe_flatten_since is not None:
            if int(now_ms) - int(global_state.safe_flatten_since) >= max(0, int(cfg.halt_timeout_ms)):
                global_state.set_mode(GlobalSafetyMode.HALT, now_ms)

    def _action_for_mode(self, mode: GlobalSafetyMode) -> Action:
        if mode == GlobalSafetyMode.SAFE_HOLD:
            return Action(
                kind=ActionKind.FORCE_HOLD,
                execution_permit=ExecutionPermit.REDUCE_ONLY,
            )
        if mode == GlobalSafetyMode.SAFE_FLATTEN:
            return Action(
                kind=ActionKind.FORCE_FLATTEN,
                target_override=0.0,
                execution_permit=ExecutionPermit.REDUCE_ONLY,
            )
        if mode == GlobalSafetyMode.HALT:
            return Action(
                kind=ActionKind.HALT,
                execution_permit=ExecutionPermit.BLOCK,
            )
        return Action(
            kind=ActionKind.CONTINUE,
            execution_permit=ExecutionPermit.FULL,
        )

    def _domain_cfg_for(self, cfg: FaultPolicyCfg, domain: str) -> DomainPolicyCfg:
        found = cfg.domains.get(domain)
        if found is not None:
            return found
        return DomainPolicyCfg(
            criticality=cfg.unknown_domain_criticality,
            degrade_threshold=1,
            circuit_threshold=1,
            max_restarts=0,
            staleness_ms=0,
            staleness_session_aware=False,
            restart_delay_base_ms=5_000,
        )

    def _infer_severity(
        self,
        event: FaultEvent,
        domain_cfg: DomainPolicyCfg,
        domain_health: DomainHealth,
    ) -> Severity:
        if event.kind == FaultKind.UNKNOWN:
            return "degraded"
        fault_count = int(domain_health.fault_count) + 1
        degrade_threshold = max(1, int(domain_cfg.degrade_threshold))
        circuit_threshold = max(degrade_threshold, int(domain_cfg.circuit_threshold))
        if fault_count >= circuit_threshold and domain_cfg.criticality == "hard":
            return "fatal"
        if fault_count >= degrade_threshold:
            return "degraded"
        return "transient"

    def _apply_hint(self, inferred: Severity, hint: str | None) -> Severity:
        if hint not in _SEVERITY_RANK:
            return inferred
        # Invariant: hints may escalate inferred severity but never downgrade it.
        if _SEVERITY_RANK[hint] > _SEVERITY_RANK[inferred]:
            return hint
        return inferred

    def _permit_for_mode(self, mode: GlobalSafetyMode) -> ExecutionPermit:
        if mode == GlobalSafetyMode.HALT:
            return ExecutionPermit.BLOCK
        if mode in (GlobalSafetyMode.SAFE_HOLD, GlobalSafetyMode.SAFE_FLATTEN):
            return ExecutionPermit.REDUCE_ONLY
        return ExecutionPermit.FULL
