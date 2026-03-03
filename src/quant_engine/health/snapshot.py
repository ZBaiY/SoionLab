from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from quant_engine.health.events import ExecutionPermit


class GlobalSafetyMode(str, Enum):
    RUNNING = "running"
    DEGRADED = "degraded"
    SAFE_HOLD = "safe_hold"
    SAFE_FLATTEN = "safe_flatten"
    HALT = "halt"


class DomainHealthState(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CIRCUIT_OPEN = "circuit_open"
    RECOVERING = "recovering"


@dataclass(frozen=True)
class DomainHealthSummary:
    state: DomainHealthState
    fault_count: int
    total_faults: int
    last_tick_ts: int | None
    staleness_ms: int | None
    recovery_attempts: int
    probe_ok_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "fault_count": int(self.fault_count),
            "total_faults": int(self.total_faults),
            "last_tick_ts": None if self.last_tick_ts is None else int(self.last_tick_ts),
            "staleness_ms": None if self.staleness_ms is None else int(self.staleness_ms),
            "recovery_attempts": int(self.recovery_attempts),
            "probe_ok_count": int(self.probe_ok_count),
        }


@dataclass(frozen=True)
class HealthSnapshot:
    ts: int
    global_mode: GlobalSafetyMode
    domains: dict[str, DomainHealthSummary]
    consecutive_skips: int
    consecutive_step_failures: int
    last_step_ts: int | None
    execution_permit: ExecutionPermit

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": int(self.ts),
            "global_mode": self.global_mode.value,
            "domains": {k: v.to_dict() for k, v in self.domains.items()},
            "consecutive_skips": int(self.consecutive_skips),
            "consecutive_step_failures": int(self.consecutive_step_failures),
            "last_step_ts": None if self.last_step_ts is None else int(self.last_step_ts),
            "execution_permit": self.execution_permit.value,
        }
