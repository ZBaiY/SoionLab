from quant_engine.health.config import DomainPolicyCfg
from quant_engine.health.snapshot import DomainHealthState
from quant_engine.health.state import DomainHealth


def _cfg(**overrides) -> DomainPolicyCfg:
    base = DomainPolicyCfg(
        criticality="hard",
        degrade_threshold=2,
        circuit_threshold=5,
        max_restarts=10,
        staleness_ms=60_000,
        staleness_session_aware=True,
        restart_delay_base_ms=5_000,
        probe_success_count=3,
        probe_timeout_ms=30_000,
        restart_backoff_factor=2.0,
        restart_max_delay_ms=300_000,
    )
    values = {**base.__dict__, **overrides}
    return DomainPolicyCfg(**values)


def test_domain_healthy_to_degraded() -> None:
    state = DomainHealth("ohlcv", "BTCUSDT")
    cfg = _cfg(degrade_threshold=2, circuit_threshold=5)
    state.apply_fault(cfg, now_ms=1)
    assert state.state == DomainHealthState.HEALTHY
    state.apply_fault(cfg, now_ms=2)
    assert state.state == DomainHealthState.DEGRADED


def test_domain_degraded_to_circuit_open() -> None:
    state = DomainHealth("ohlcv", "BTCUSDT")
    cfg = _cfg(degrade_threshold=1, circuit_threshold=2)
    state.apply_fault(cfg, now_ms=1)
    assert state.state == DomainHealthState.DEGRADED
    state.apply_fault(cfg, now_ms=2)
    assert state.state == DomainHealthState.CIRCUIT_OPEN


def test_domain_recovery_probe_graduation() -> None:
    state = DomainHealth("ohlcv", "BTCUSDT")
    cfg = _cfg(degrade_threshold=1, circuit_threshold=2, probe_success_count=3)
    state.apply_fault(cfg, now_ms=1)
    state.apply_fault(cfg, now_ms=2)
    assert state.state == DomainHealthState.CIRCUIT_OPEN
    state.apply_restart(cfg, now_ms=3)
    assert state.state == DomainHealthState.RECOVERING
    state.apply_tick_ok(cfg)
    state.apply_tick_ok(cfg)
    assert state.state == DomainHealthState.RECOVERING
    state.apply_tick_ok(cfg)
    assert state.state == DomainHealthState.HEALTHY
    assert state.probe_ok_count == 0


def test_domain_recovery_probe_fault_resets() -> None:
    state = DomainHealth("ohlcv", "BTCUSDT")
    cfg = _cfg(degrade_threshold=1, circuit_threshold=2, restart_delay_base_ms=5_000)
    state.apply_fault(cfg, now_ms=1)
    state.apply_fault(cfg, now_ms=2)
    state.apply_restart(cfg, now_ms=3)
    state.apply_tick_ok(cfg)
    state.apply_fault(cfg, now_ms=4)
    assert state.state == DomainHealthState.CIRCUIT_OPEN
    assert state.restart_delay_ms == 10_000


def test_domain_recovery_probe_timeout() -> None:
    state = DomainHealth("ohlcv", "BTCUSDT")
    cfg = _cfg(degrade_threshold=1, circuit_threshold=2, probe_timeout_ms=1_000)
    state.apply_fault(cfg, now_ms=1)
    state.apply_fault(cfg, now_ms=2)
    state.apply_restart(cfg, now_ms=10)
    assert state.state == DomainHealthState.RECOVERING
    timed_out = state.check_probe_timeout(2_020, cfg)
    assert timed_out is True
    assert state.state == DomainHealthState.CIRCUIT_OPEN


def test_domain_recovery_thrashing_backoff() -> None:
    state = DomainHealth("ohlcv", "BTCUSDT")
    cfg = _cfg(
        degrade_threshold=1,
        circuit_threshold=2,
        restart_delay_base_ms=5_000,
        restart_backoff_factor=2.0,
        restart_max_delay_ms=300_000,
    )
    delays: list[int] = []
    for i in range(6):
        state.apply_fault(cfg, now_ms=i * 10 + 1)
        state.apply_fault(cfg, now_ms=i * 10 + 2)
        state.apply_restart(cfg, now_ms=i * 10 + 3)
        state.apply_fault(cfg, now_ms=i * 10 + 4)
        delays.append(state.restart_delay_ms)
    assert delays == [10_000, 20_000, 40_000, 80_000, 160_000, 300_000]


def test_domain_degraded_tick_ok_resets() -> None:
    state = DomainHealth("ohlcv", "BTCUSDT")
    cfg = _cfg(degrade_threshold=1, circuit_threshold=5)
    state.apply_fault(cfg, now_ms=1)
    assert state.state == DomainHealthState.DEGRADED
    state.apply_tick_ok(cfg)
    assert state.state == DomainHealthState.HEALTHY
    assert state.fault_count == 0
