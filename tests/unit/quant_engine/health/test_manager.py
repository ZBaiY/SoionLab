from dataclasses import asdict

from quant_engine.health import HealthManager, default_realtime_config
from quant_engine.health.events import Action, FaultEvent, FaultKind
from quant_engine.health.state import make_health_key
from quant_engine.utils.logger import get_logger


def test_report_returns_action() -> None:
    mgr = HealthManager(cfg=default_realtime_config(), logger=get_logger(__name__))
    action = mgr.report(
        FaultEvent(
            ts=1,
            source="test",
            kind=FaultKind.SOURCE_EXCEPTION,
            domain="trades",
            symbol="BTCUSDT",
        )
    )
    assert isinstance(action, Action)


def test_snapshot_serializable() -> None:
    mgr = HealthManager(cfg=default_realtime_config(), logger=get_logger(__name__))
    mgr.report_tick("ohlcv", "BTCUSDT", 1000)
    snapshot = mgr.snapshot()
    payload = asdict(snapshot)
    assert isinstance(payload, dict)
    assert payload["domains"]


def test_make_health_key_centralized() -> None:
    assert make_health_key(
        FaultEvent(
            ts=1,
            source="test",
            kind=FaultKind.UNKNOWN,
            domain="ohlcv",
            symbol="BTCUSDT",
        )
    ) == ("ohlcv", "BTCUSDT")
    assert make_health_key(
        FaultEvent(
            ts=1,
            source="test",
            kind=FaultKind.UNKNOWN,
            domain=None,
            symbol=None,
        )
    ) == ("__engine__", "__all__")
