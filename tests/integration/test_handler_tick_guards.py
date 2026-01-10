from __future__ import annotations

from ingestion.contracts.tick import IngestionTick
from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.runtime.modes import EngineMode


def test_handler_tick_guards() -> None:
    handler = OHLCVDataHandler("BTCUSDT", interval="1m", mode=EngineMode.MOCK, source_id="mock-source")
    anchor_ts = 1_700_000_000_000
    handler.align_to(anchor_ts)

    base_payload = {
        "open": 1.0,
        "high": 1.1,
        "low": 0.9,
        "close": 1.0,
        "volume": 10.0,
        "data_ts": anchor_ts,
    }
    source_id = handler.source_id
    correct_tick = IngestionTick(
        timestamp=anchor_ts,
        data_ts=anchor_ts,
        domain="ohlcv",
        symbol="BTCUSDT",
        payload={**base_payload, "sentinel": "ok"},
        source_id=source_id,
    )
    wrong_symbol_tick = IngestionTick(
        timestamp=anchor_ts,
        data_ts=anchor_ts,
        domain="ohlcv",
        symbol="ETHUSDT",
        payload={**base_payload, "sentinel": "wrong-symbol"},
        source_id=source_id,
    )
    wrong_domain_tick = IngestionTick(
        timestamp=anchor_ts,
        data_ts=anchor_ts,
        domain="orderbook",
        symbol="BTCUSDT",
        payload={**base_payload, "sentinel": "wrong-domain"},
        source_id=source_id,
    )
    wrong_source_tick = IngestionTick(
        timestamp=anchor_ts,
        data_ts=anchor_ts,
        domain="ohlcv",
        symbol="BTCUSDT",
        payload={**base_payload, "sentinel": "wrong-source"},
        source_id=f"{source_id}-other" if source_id is not None else "other",
    )

    handler.on_new_tick(correct_tick)
    assert len(handler.cache.buffer) == 1
    snap = handler.cache.last()
    assert snap is not None
    assert snap.aux.get("sentinel") == "ok"

    handler.on_new_tick(wrong_symbol_tick)
    assert len(handler.cache.buffer) == 1
    snap = handler.cache.last()
    assert snap is not None
    assert snap.aux.get("sentinel") == "ok"

    handler.on_new_tick(wrong_domain_tick)
    assert len(handler.cache.buffer) == 1
    snap = handler.cache.last()
    assert snap is not None
    assert snap.aux.get("sentinel") == "ok"

    handler.on_new_tick(wrong_source_tick)
    assert len(handler.cache.buffer) == 1
    snap = handler.cache.last()
    assert snap is not None
    assert snap.aux.get("sentinel") == "ok"
