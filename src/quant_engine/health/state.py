from __future__ import annotations

import time

from quant_engine.health.config import DomainPolicyCfg
from quant_engine.health.events import FaultEvent
from quant_engine.health.snapshot import DomainHealthState, DomainHealthSummary, GlobalSafetyMode


def make_health_key(event: FaultEvent) -> tuple[str, str]:
    return (event.domain or "__engine__", event.symbol or "__all__")


class DomainHealth:
    def __init__(self, domain: str, symbol: str):
        self.domain = str(domain)
        self.symbol = str(symbol)
        self.state = DomainHealthState.HEALTHY
        self.fault_count = 0
        self.total_faults = 0
        self.recovery_attempts = 0
        self.probe_ok_count = 0
        self.probe_start_ts: int | None = None
        self.restart_delay_ms = 0
        self.last_tick_ts: int | None = None
        self.circuit_open_since: int | None = None

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    def _base_restart_delay(self, cfg: DomainPolicyCfg) -> int:
        return max(0, int(cfg.restart_delay_base_ms))

    def _ensure_restart_delay(self, cfg: DomainPolicyCfg) -> None:
        if int(self.restart_delay_ms) <= 0:
            self.restart_delay_ms = self._base_restart_delay(cfg)

    def _increase_backoff(self, cfg: DomainPolicyCfg) -> None:
        base = self._base_restart_delay(cfg)
        cap = max(base, int(cfg.restart_max_delay_ms))
        factor = float(cfg.restart_backoff_factor)
        self._ensure_restart_delay(cfg)
        grown = int(round(float(self.restart_delay_ms) * max(1.0, factor)))
        self.restart_delay_ms = min(max(base, grown), cap)

    def _transition_to_healthy(self, cfg: DomainPolicyCfg) -> None:
        self.state = DomainHealthState.HEALTHY
        self.fault_count = 0
        self.recovery_attempts = 0
        self.probe_ok_count = 0
        self.probe_start_ts = None
        self.circuit_open_since = None
        self.restart_delay_ms = self._base_restart_delay(cfg)

    def _transition_to_circuit_open(self, cfg: DomainPolicyCfg, now_ms: int) -> None:
        self.state = DomainHealthState.CIRCUIT_OPEN
        self.circuit_open_since = int(now_ms)
        self.probe_ok_count = 0
        self.probe_start_ts = None
        self._ensure_restart_delay(cfg)

    def apply_fault(self, cfg: DomainPolicyCfg, *, now_ms: int | None = None) -> DomainHealthState:
        now = int(now_ms) if now_ms is not None else self._now_ms()
        degrade_threshold = max(1, int(cfg.degrade_threshold))
        circuit_threshold = max(degrade_threshold, int(cfg.circuit_threshold))
        self.total_faults += 1
        self.fault_count += 1

        if self.state == DomainHealthState.HEALTHY:
            if self.fault_count >= degrade_threshold:
                self.state = DomainHealthState.DEGRADED
            return self.state

        if self.state == DomainHealthState.DEGRADED:
            if self.fault_count >= circuit_threshold:
                self._transition_to_circuit_open(cfg, now)
            return self.state

        if self.state == DomainHealthState.RECOVERING:
            self._transition_to_circuit_open(cfg, now)
            self._increase_backoff(cfg)
            return self.state

        return self.state

    def apply_tick_ok(self, cfg: DomainPolicyCfg) -> DomainHealthState:
        if self.state == DomainHealthState.DEGRADED:
            self._transition_to_healthy(cfg)
            return self.state

        if self.state == DomainHealthState.RECOVERING:
            self.probe_ok_count += 1
            needed = max(1, int(cfg.probe_success_count))
            if self.probe_ok_count >= needed:
                self._transition_to_healthy(cfg)
        return self.state

    def apply_restart(self, cfg: DomainPolicyCfg, *, now_ms: int | None = None) -> DomainHealthState:
        if self.state != DomainHealthState.CIRCUIT_OPEN:
            return self.state
        if self.recovery_attempts >= max(0, int(cfg.max_restarts)):
            return self.state
        now = int(now_ms) if now_ms is not None else self._now_ms()
        self.recovery_attempts += 1
        self.state = DomainHealthState.RECOVERING
        self.probe_ok_count = 0
        self.probe_start_ts = now
        return self.state

    def check_probe_timeout(self, now_ms: int, cfg: DomainPolicyCfg) -> bool:
        if self.state != DomainHealthState.RECOVERING:
            return False
        if self.probe_start_ts is None:
            return False
        timeout_ms = max(0, int(cfg.probe_timeout_ms))
        if int(now_ms) - int(self.probe_start_ts) <= timeout_ms:
            return False
        self._transition_to_circuit_open(cfg, int(now_ms))
        self._increase_backoff(cfg)
        return True

    def to_summary(self, now_ms: int) -> DomainHealthSummary:
        staleness_ms = None
        if self.last_tick_ts is not None:
            staleness_ms = max(0, int(now_ms) - int(self.last_tick_ts))
        return DomainHealthSummary(
            state=self.state,
            fault_count=int(self.fault_count),
            total_faults=int(self.total_faults),
            last_tick_ts=None if self.last_tick_ts is None else int(self.last_tick_ts),
            staleness_ms=staleness_ms,
            recovery_attempts=int(self.recovery_attempts),
            probe_ok_count=int(self.probe_ok_count),
        )


class GlobalSafety:
    def __init__(self) -> None:
        self.mode = GlobalSafetyMode.RUNNING
        self.consecutive_skips = 0
        self.consecutive_step_failures = 0
        self.last_step_ts: int | None = None
        self.safe_hold_since: int | None = None
        self.safe_flatten_since: int | None = None

    def set_mode(self, mode: GlobalSafetyMode, ts: int) -> None:
        if self.mode != mode:
            self.mode = mode
        if mode == GlobalSafetyMode.SAFE_HOLD:
            if self.safe_hold_since is None:
                self.safe_hold_since = int(ts)
            self.safe_flatten_since = None
            return
        if mode == GlobalSafetyMode.SAFE_FLATTEN:
            if self.safe_flatten_since is None:
                self.safe_flatten_since = int(ts)
            return
        if mode in (GlobalSafetyMode.RUNNING, GlobalSafetyMode.DEGRADED):
            self.safe_hold_since = None
            self.safe_flatten_since = None

    def record_step_ok(self, ts: int) -> None:
        self.last_step_ts = int(ts)
        self.consecutive_skips = 0
        self.consecutive_step_failures = 0

    def record_step_skipped(self, ts: int) -> None:
        self.last_step_ts = int(ts)
        self.consecutive_skips += 1

    def record_step_failure(self, ts: int) -> None:
        self.last_step_ts = int(ts)
        self.consecutive_step_failures += 1
