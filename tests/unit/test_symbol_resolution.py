import pandas as pd
import pytest

from ingestion.contracts.tick import IngestionTick
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.ohlcv.realtime import OHLCVDataHandler


def test_option_chain_fuzzy_symbol_matching() -> None:
    handler = OptionChainDataHandler(symbol="BTCUSDT", interval="1m", asset="BTC")
    ts1 = 1_700_000_000_000
    df = pd.DataFrame(
        [
            {
                "instrument_name": "BTC-30JUN23-20000-C",
                "expiry_ts": ts1 + 86_400_000,
                "strike": 20_000,
                "option_type": "call",
            }
        ]
    )
    payload1 = {"data_ts": ts1, "chain": df}
    tick1 = IngestionTick(
        timestamp=ts1,
        data_ts=ts1,
        domain="option_chain",
        symbol="BTCUSDT",
        payload=payload1,
        source_id="DERIBIT",
    )
    handler.on_new_tick(tick1)
    assert handler.last_timestamp() == ts1

    ts2 = ts1 + 60_000
    payload2 = {"data_ts": ts2, "chain": df}
    tick2 = IngestionTick(
        timestamp=ts2,
        data_ts=ts2,
        domain="option_chain",
        symbol="BTCUSDT",
        payload=payload2,
        source_id="DERIBIT",
    )
    handler.on_new_tick(tick2)
    assert handler.last_timestamp() == ts2


def test_ohlcv_strict_symbol_and_requires_quote() -> None:
    with pytest.raises(ValueError):
        OHLCVDataHandler(symbol="BTC", interval="1m")

    handler = OHLCVDataHandler(symbol="BTCUSDT", interval="1m", currency="USDT")
    ts = 1_700_000_000_000
    handler.align_to(ts)
    payload = {
        "data_ts": ts,
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "volume": 1.0,
    }
    tick_bad = IngestionTick(
        timestamp=ts,
        data_ts=ts,
        domain="ohlcv",
        symbol="BTC",
        payload=payload,
    )
    handler.on_new_tick(tick_bad)
    assert handler.last_timestamp() is None

    tick_ok = IngestionTick(
        timestamp=ts,
        data_ts=ts,
        domain="ohlcv",
        symbol="BTCUSDT",
        payload=payload,
    )
    handler.on_new_tick(tick_ok)
    assert handler.last_timestamp() == ts
