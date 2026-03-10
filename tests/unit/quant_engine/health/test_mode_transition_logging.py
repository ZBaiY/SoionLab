"""Tests for health manager mode transition logging."""

from __future__ import annotations

import logging

from quant_engine.health import HealthManager, default_realtime_config
from quant_engine.health.config import FaultPolicyCfg
from quant_engine.health.events import FaultEvent, FaultKind
from quant_engine.health.snapshot import GlobalSafetyMode
from quant_engine.utils.logger import get_logger

_LOGGER_NAME = "test.health.transition"


def _make_mgr(cfg: FaultPolicyCfg | None = None) -> HealthManager:
    return HealthManager(cfg=cfg or default_realtime_config(), logger=get_logger(_LOGGER_NAME))


def _fault(domain: str = "ohlcv", symbol: str = "BTCUSDT", ts: int = 1) -> FaultEvent:
    return FaultEvent(
        ts=ts,
        source="test",
        kind=FaultKind.SOURCE_EXCEPTION,
        domain=domain,
        symbol=symbol,
    )


def _transition_records(caplog):
    return [r for r in caplog.records if r.msg == "health.global_mode.transition"]


class TestModeTransitionLogging:
    def test_running_to_degraded_logged(self, caplog) -> None:
        mgr = _make_mgr()
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            mgr.report(_fault(ts=1))
            mgr.report(_fault(ts=2))
        transitions = _transition_records(caplog)
        assert len(transitions) >= 1
        ctx = getattr(transitions[0], "context", {})
        assert ctx["prev_mode"] == "RUNNING"
        assert ctx["new_mode"] == "DEGRADED"

    def test_degraded_to_safe_hold_logged(self, caplog) -> None:
        mgr = _make_mgr()
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            for i in range(10):
                mgr.report(_fault(ts=i + 1))
        transitions = _transition_records(caplog)
        mode_names = [getattr(r, "context", {}).get("new_mode") for r in transitions]
        assert "DEGRADED" in mode_names
        assert "SAFE_HOLD" in mode_names

    def test_skip_escalation_logged(self, caplog) -> None:
        mgr = _make_mgr()
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            for i in range(6):
                mgr.report_step_skipped(ts=i + 1)
        transitions = _transition_records(caplog)
        assert len(transitions) >= 1
        # Should include reason
        ctx = getattr(transitions[0], "context", {})
        assert ctx.get("reason") == "consecutive_skips"

    def test_time_escalation_safe_hold_to_flatten_logged(self, caplog) -> None:
        cfg = default_realtime_config()
        mgr = _make_mgr(cfg)
        # Force into SAFE_HOLD via hard domain circuit
        for i in range(10):
            mgr.report(_fault(ts=i + 1))
        assert mgr.global_mode() == GlobalSafetyMode.SAFE_HOLD

        # Advance time past flatten_timeout_ms
        future_ts = 10 + cfg.flatten_timeout_ms + 1
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            mgr.check_staleness(future_ts)
        transitions = _transition_records(caplog)
        flatten = [r for r in transitions if getattr(r, "context", {}).get("new_mode") == "SAFE_FLATTEN"]
        assert len(flatten) >= 1
        ctx = getattr(flatten[0], "context", {})
        assert ctx["prev_mode"] == "SAFE_HOLD"

    def test_recovery_to_running_logged(self, caplog) -> None:
        mgr = _make_mgr()
        # Push to DEGRADED
        mgr.report(_fault(ts=1))
        mgr.report(_fault(ts=2))
        assert mgr.global_mode() == GlobalSafetyMode.DEGRADED

        # Recover via tick_ok
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            mgr.report_tick("ohlcv", "BTCUSDT", ts=3)
        transitions = _transition_records(caplog)
        running = [r for r in transitions if getattr(r, "context", {}).get("new_mode") == "RUNNING"]
        assert len(running) >= 1

    def test_no_log_when_mode_unchanged(self, caplog) -> None:
        mgr = _make_mgr()
        with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
            mgr.report_step_ok(ts=1)
            mgr.report_step_ok(ts=2)
        transitions = _transition_records(caplog)
        assert len(transitions) == 0
