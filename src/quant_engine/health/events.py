from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal


SeverityHint = Literal["transient", "degraded", "fatal"]


class FaultKind(str, Enum):
    # Ingestion layer
    SOURCE_EXCEPTION = "source_exception"
    SOURCE_EXHAUSTED = "source_exhausted"
    TICK_MALFORMED = "tick_malformed"
    TICK_STALE = "tick_stale"

    # Handler/cache layer
    HANDLER_EXCEPTION = "handler_exception"
    BOOTSTRAP_FAILURE = "bootstrap_failure"
    BACKFILL_FAILURE = "backfill_failure"

    # Engine step stages
    FEATURE_EXCEPTION = "feature_exception"
    MODEL_EXCEPTION = "model_exception"
    DECISION_EXCEPTION = "decision_exception"
    RISK_EXCEPTION = "risk_exception"
    EXECUTION_EXCEPTION = "execution_exception"
    PORTFOLIO_EXCEPTION = "portfolio_exception"
    SNAPSHOT_EXCEPTION = "snapshot_exception"

    # Guardrails
    MONOTONICITY_VIOLATION = "monotonicity_violation"
    LOOKAHEAD_VIOLATION = "lookahead_violation"
    SCHEMA_DRIFT = "schema_drift"
    STEP_ORDER_VIOLATION = "step_order_violation"

    # Operational
    WRITER_EXCEPTION = "writer_exception"
    READINESS_SKIP = "readiness_skip"
    STEP_TIMEOUT = "step_timeout"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class FaultEvent:
    ts: int
    source: str
    kind: FaultKind
    domain: str | None = None
    symbol: str | None = None
    severity_hint: SeverityHint | None = None
    exc_type: str | None = None
    exc_msg: str | None = None
    scope: dict[str, str] | None = None
    context: dict[str, Any] | None = None


class ActionKind(str, Enum):
    CONTINUE = "continue"
    SKIP_STEP = "skip_step"
    SKIP_DOMAIN = "skip_domain"
    FORCE_HOLD = "force_hold"
    FORCE_FLATTEN = "force_flatten"
    RESTART_SOURCE = "restart_source"
    DISABLE_DOMAIN = "disable_domain"
    HALT = "halt"
    LOG_ONLY = "log_only"


class ExecutionPermit(str, Enum):
    FULL = "full"
    REDUCE_ONLY = "reduce_only"
    BLOCK = "block"


@dataclass(frozen=True)
class Action:
    kind: ActionKind
    domain: str | None = None
    symbol: str | None = None
    detail: str | None = None
    target_override: float | None = None
    execution_permit: ExecutionPermit = ExecutionPermit.FULL
