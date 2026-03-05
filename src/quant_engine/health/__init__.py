"""Public health/fault-control API surface used by runtime and strategy wiring."""

from quant_engine.health.config import DomainPolicyCfg, FaultPolicyCfg, backtest_config, default_realtime_config
from quant_engine.health.events import Action, ActionKind, ExecutionPermit, FaultEvent, FaultKind
from quant_engine.health.manager import HealthManager
from quant_engine.health.snapshot import (
    DomainHealthState,
    DomainHealthSummary,
    GlobalSafetyMode,
    HealthSnapshot,
)

__all__ = [
    "Action",
    "ActionKind",
    "ExecutionPermit",
    "FaultEvent",
    "FaultKind",
    "DomainPolicyCfg",
    "FaultPolicyCfg",
    "HealthManager",
    "HealthSnapshot",
    "DomainHealthSummary",
    "GlobalSafetyMode",
    "DomainHealthState",
    "default_realtime_config",
    "backtest_config",
]
