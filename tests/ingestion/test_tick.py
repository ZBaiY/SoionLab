from __future__ import annotations

from typing import get_args

from ingestion.contracts.tick import Domain, IngestionTick, normalize_tick


def test_ingestion_tick_properties_and_repr() -> None:
    payload = {"price": 101.5}
    tick = IngestionTick(
        timestamp=1_700_000_000_000,
        data_ts=1_700_000_000_123,
        domain="ohlcv",
        symbol="BTCUSDT",
        payload=payload,
    )

    assert isinstance(tick.timestamp, int)
    assert isinstance(tick.data_ts, int)
    assert tick.event_ts == tick.data_ts
    assert tick.arrival_ts == tick.timestamp
    assert tick.domain in set(get_args(Domain))
    assert tick.payload is payload

    rendered = repr(tick)
    text = str(tick)
    for out in (rendered, text):
        assert "domain" in out
        assert "symbol" in out
        assert "data_ts" in out


def test_normalize_tick_coerces_epoch_ms_and_preserves_payload() -> None:
    payload = {"k": "v"}
    tick = normalize_tick(
        timestamp=1_700_000_000.5,  # seconds -> ms
        data_ts=1_700_000_001_234,  # already ms
        domain="trades",
        symbol="ETHUSDT",
        payload=payload,
    )

    assert isinstance(tick.timestamp, int)
    assert isinstance(tick.data_ts, int)
    assert tick.event_ts == tick.data_ts
    assert tick.arrival_ts == tick.timestamp
    assert tick.domain in set(get_args(Domain))
    assert tick.payload is payload


def test_normalize_tick_defaults_data_ts_to_timestamp() -> None:
    tick = normalize_tick(
        timestamp=1_700_000_000,
        domain="sentiment",
        symbol="BTCUSDT",
        payload={"sentiment": 0.1},
    )
    assert tick.data_ts == tick.timestamp
