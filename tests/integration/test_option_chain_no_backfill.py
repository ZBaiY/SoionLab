from __future__ import annotations

import logging

import pandas as pd
import pytest

from ingestion.contracts.tick import IngestionTick
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.runtime.modes import EngineMode


def test_option_chain_maybe_backfill_is_noop_in_realtime(caplog: pytest.LogCaptureFixture) -> None:
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", mode=EngineMode.REALTIME, preset="option_chain")
    ts = 1_700_000_000_000
    tick = IngestionTick(
        timestamp=ts,
        data_ts=ts,
        domain="option_chain",
        symbol="BTC",
        payload={
            "data_ts": ts,
            "frame": pd.DataFrame(
                [
                    {
                        "instrument_name": "BTC-1-EXP-C",
                        "expiration_timestamp": ts + 86_400_000,
                        "strike": 10_000,
                        "option_type": "call",
                        "bid_price": 1.0,
                        "ask_price": 1.1,
                    }
                ]
            ),
        },
        source_id=getattr(handler, "source_id", None),
    )
    handler.on_new_tick(tick)
    with caplog.at_level(logging.WARNING):
        handler._maybe_backfill(target_ts=ts + 10 * 60_000)
    assert handler._should_backfill() is False
    assert not any("option_chain.backfill.cold_start" in rec.getMessage() for rec in caplog.records)
    assert not any("option_chain.gap_detected" in rec.getMessage() for rec in caplog.records)

