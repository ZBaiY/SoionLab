from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class DomainPolicyCfg:
    # Role: hard domains can escalate global safety more aggressively than soft domains.
    criticality: Literal["hard", "soft"]
    degrade_threshold: int
    circuit_threshold: int
    max_restarts: int
    staleness_ms: int
    staleness_session_aware: bool
    restart_delay_base_ms: int
    probe_success_count: int = 3
    probe_timeout_ms: int = 30_000
    restart_backoff_factor: float = 2.0
    restart_max_delay_ms: int = 300_000


@dataclass(frozen=True)
class FaultPolicyCfg:
    domains: dict[str, DomainPolicyCfg]
    skip_threshold: int = 5
    step_fail_threshold: int = 3
    hard_fail_threshold: int = 10
    soft_circuit_escalation_count: int = 2
    flatten_timeout_ms: int = 300_000
    halt_timeout_ms: int = 600_000
    cooldown_ms: int = 60_000
    unknown_domain_criticality: Literal["hard", "soft"] = "hard"


def default_realtime_config(interval_ms: int = 60_000) -> FaultPolicyCfg:
    i = max(1, int(interval_ms))
    # Role: domain staleness thresholds are scaled from strategy interval to keep timing semantics consistent.
    domains = {
        "ohlcv": DomainPolicyCfg(
            criticality="hard",
            degrade_threshold=2,
            circuit_threshold=5,
            max_restarts=3,
            staleness_ms=2 * i,
            staleness_session_aware=True,
            restart_delay_base_ms=5_000,
        ),
        "orderbook": DomainPolicyCfg(
            criticality="hard",
            degrade_threshold=3,
            circuit_threshold=8,
            max_restarts=3,
            staleness_ms=3 * i,
            staleness_session_aware=True,
            restart_delay_base_ms=5_000,
        ),
        "trades": DomainPolicyCfg(
            criticality="soft",
            degrade_threshold=5,
            circuit_threshold=15,
            max_restarts=3,
            staleness_ms=5 * i,
            staleness_session_aware=True,
            restart_delay_base_ms=5_000,
        ),
        "option_chain": DomainPolicyCfg(
            criticality="soft",
            degrade_threshold=5,
            circuit_threshold=15,
            max_restarts=3,
            staleness_ms=5 * i,
            staleness_session_aware=True,
            restart_delay_base_ms=5_000,
        ),
        "option_trades": DomainPolicyCfg(
            criticality="soft",
            degrade_threshold=5,
            circuit_threshold=15,
            max_restarts=3,
            staleness_ms=10 * i,
            staleness_session_aware=True,
            restart_delay_base_ms=5_000,
        ),
        "sentiment": DomainPolicyCfg(
            criticality="soft",
            degrade_threshold=5,
            circuit_threshold=15,
            max_restarts=3,
            staleness_ms=300_000,
            staleness_session_aware=False,
            restart_delay_base_ms=5_000,
            probe_timeout_ms=60_000,
        ),
        "iv_surface": DomainPolicyCfg(
            criticality="soft",
            degrade_threshold=5,
            circuit_threshold=15,
            max_restarts=3,
            staleness_ms=5 * i,
            staleness_session_aware=True,
            restart_delay_base_ms=5_000,
        ),
        "execution/binance": DomainPolicyCfg(
            criticality="hard",
            degrade_threshold=2,
            circuit_threshold=5,
            max_restarts=0,
            staleness_ms=0,
            staleness_session_aware=False,
            restart_delay_base_ms=0,
        ),
    }
    return FaultPolicyCfg(domains=domains)


def backtest_config(interval_ms: int = 60_000) -> FaultPolicyCfg:
    realtime = default_realtime_config(interval_ms=interval_ms)
    # Scenario: backtest mode collapses thresholds to fail fast and expose deterministic regressions.
    domains = {
        name: DomainPolicyCfg(
            criticality=cfg.criticality,
            degrade_threshold=1,
            circuit_threshold=1,
            max_restarts=1,
            staleness_ms=0,
            staleness_session_aware=cfg.staleness_session_aware,
            restart_delay_base_ms=cfg.restart_delay_base_ms,
            probe_success_count=cfg.probe_success_count,
            probe_timeout_ms=cfg.probe_timeout_ms,
            restart_backoff_factor=cfg.restart_backoff_factor,
            restart_max_delay_ms=cfg.restart_max_delay_ms,
        )
        for name, cfg in realtime.domains.items()
    }
    return FaultPolicyCfg(
        domains=domains,
        skip_threshold=1,
        step_fail_threshold=1,
        hard_fail_threshold=1,
        soft_circuit_escalation_count=1,
        flatten_timeout_ms=0,
        halt_timeout_ms=0,
        cooldown_ms=0,
        unknown_domain_criticality=realtime.unknown_domain_criticality,
    )
