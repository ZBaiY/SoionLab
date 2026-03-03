from quant_engine.health.session import market_is_closed


def test_market_closed_pauses_staleness() -> None:
    # Saturday, January 3, 2026 15:00:00 UTC
    saturday_ts = 1767452400000
    assert market_is_closed(saturday_ts, {"calendar": "XNYS"}) is True


def test_calendar_failure_returns_false() -> None:
    assert market_is_closed(1_700_000_000_000, None) is False


def test_never_raises() -> None:
    assert market_is_closed(1_700_000_000_000, {"calendar": object()}) is False
