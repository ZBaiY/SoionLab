from quant_engine.health.config import FaultPolicyCfg, backtest_config, default_realtime_config
from quant_engine.health.events import ActionKind, ExecutionPermit, FaultEvent, FaultKind
from quant_engine.health.policy import FaultPolicy
from quant_engine.health.snapshot import GlobalSafetyMode
from quant_engine.health.state import GlobalSafety


def test_guardrail_always_halt() -> None:
    policy = FaultPolicy()
    cfg = default_realtime_config()
    guardrail_kinds = (
        FaultKind.MONOTONICITY_VIOLATION,
        FaultKind.LOOKAHEAD_VIOLATION,
        FaultKind.SCHEMA_DRIFT,
        FaultKind.STEP_ORDER_VIOLATION,
    )
    for kind in guardrail_kinds:
        domains = {}
        global_state = GlobalSafety()
        action = policy.evaluate(
            FaultEvent(ts=1, source="test", kind=kind, domain="ohlcv", symbol="BTCUSDT"),
            domains,
            global_state,
            cfg,
        )
        assert action.kind == ActionKind.HALT
        assert action.execution_permit == ExecutionPermit.BLOCK
        assert global_state.mode == GlobalSafetyMode.HALT


def test_unknown_kind_degraded() -> None:
    policy = FaultPolicy()
    cfg = default_realtime_config()
    domains = {}
    global_state = GlobalSafety()
    action = policy.evaluate(
        FaultEvent(ts=1, source="test", kind=FaultKind.UNKNOWN, domain="trades", symbol="BTCUSDT"),
        domains,
        global_state,
        cfg,
    )
    assert action.kind in (ActionKind.CONTINUE, ActionKind.FORCE_HOLD, ActionKind.FORCE_FLATTEN)
    assert global_state.mode != GlobalSafetyMode.RUNNING


def test_unknown_domain_hard() -> None:
    policy = FaultPolicy()
    cfg = default_realtime_config()
    domains = {}
    global_state = GlobalSafety()
    action = policy.evaluate(
        FaultEvent(
            ts=1,
            source="test",
            kind=FaultKind.SOURCE_EXCEPTION,
            domain="unknown_feed",
            symbol="BTCUSDT",
        ),
        domains,
        global_state,
        cfg,
    )
    assert global_state.mode in (GlobalSafetyMode.DEGRADED, GlobalSafetyMode.SAFE_HOLD)
    assert action.execution_permit in (ExecutionPermit.FULL, ExecutionPermit.REDUCE_ONLY)


def test_severity_hint_escalates() -> None:
    policy = FaultPolicy()
    cfg = default_realtime_config()
    domains = {}
    global_state = GlobalSafety()
    action = policy.evaluate(
        FaultEvent(
            ts=1,
            source="test",
            kind=FaultKind.SOURCE_EXCEPTION,
            domain="trades",
            symbol="BTCUSDT",
            severity_hint="fatal",
        ),
        domains,
        global_state,
        cfg,
    )
    assert global_state.mode == GlobalSafetyMode.SAFE_HOLD
    assert action.execution_permit == ExecutionPermit.REDUCE_ONLY


def test_severity_hint_no_downgrade() -> None:
    policy = FaultPolicy()
    domains = {}
    global_state = GlobalSafety()
    cfg = default_realtime_config()
    # First fault degrades hard domain ohlcv.
    policy.evaluate(
        FaultEvent(ts=1, source="test", kind=FaultKind.SOURCE_EXCEPTION, domain="ohlcv", symbol="BTCUSDT"),
        domains,
        global_state,
        cfg,
    )
    action = policy.evaluate(
        FaultEvent(
            ts=2,
            source="test",
            kind=FaultKind.SOURCE_EXCEPTION,
            domain="ohlcv",
            symbol="BTCUSDT",
            severity_hint="transient",
        ),
        domains,
        global_state,
        cfg,
    )
    assert action.kind in (ActionKind.CONTINUE, ActionKind.FORCE_HOLD)
    assert global_state.mode in (GlobalSafetyMode.DEGRADED, GlobalSafetyMode.SAFE_HOLD)


def test_execution_uncertain_immediate_safe_hold() -> None:
    policy = FaultPolicy()
    cfg = default_realtime_config()
    domains = {}
    global_state = GlobalSafety()
    action = policy.evaluate(
        FaultEvent(
            ts=1,
            source="test",
            kind=FaultKind.EXECUTION_EXCEPTION,
            domain=None,
            symbol="BTCUSDT",
            context={"uncertain": True},
        ),
        domains,
        global_state,
        cfg,
    )
    assert action.kind == ActionKind.FORCE_HOLD
    assert action.execution_permit == ExecutionPermit.REDUCE_ONLY
    assert global_state.mode == GlobalSafetyMode.SAFE_HOLD


def test_execution_clean_normal_escalation() -> None:
    policy = FaultPolicy()
    cfg = default_realtime_config()
    domains = {}
    global_state = GlobalSafety()
    action = policy.evaluate(
        FaultEvent(
            ts=1,
            source="test",
            kind=FaultKind.EXECUTION_EXCEPTION,
            domain=None,
            symbol="BTCUSDT",
        ),
        domains,
        global_state,
        cfg,
    )
    assert action.kind == ActionKind.SKIP_STEP


def test_backtest_config_immediate_halt() -> None:
    policy = FaultPolicy()
    cfg: FaultPolicyCfg = backtest_config()
    domains = {}
    global_state = GlobalSafety()
    action = policy.evaluate(
        FaultEvent(ts=1, source="test", kind=FaultKind.MODEL_EXCEPTION, domain=None, symbol="BTCUSDT"),
        domains,
        global_state,
        cfg,
    )
    assert action.kind == ActionKind.HALT
