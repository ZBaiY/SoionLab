from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

import ingestion.option_chain.source as option_chain_source
from ingestion.option_chain.source import DeribitOptionChainRESTSource


def _fake_rpc_factory(*, chain_rows: list[dict], quote_rows: list[dict]):
    def _fake_rpc(method: str, params, *, session, timeout: float, url: str):
        if method == "public/get_instruments":
            return chain_rows
        if method == "public/get_book_summary_by_currency":
            return quote_rows
        raise AssertionError(f"unexpected method: {method}")

    return _fake_rpc


def _date_path(root, *, currency: str, interval: str, step_ts: int):
    dt = datetime.fromtimestamp(int(step_ts) / 1000.0, tz=timezone.utc)
    year = dt.strftime("%Y")
    ymd = dt.strftime("%Y_%m_%d")
    return root / currency / interval / year / f"{ymd}.parquet"


def test_option_chain_rpc_quote_volume_rename_and_market_ts_nan(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain_rows = [
        {
            "instrument_name": "BTC-1JAN24-10000-C",
            "expiration_timestamp": 1_700_100_000_000,
            "strike": 10_000,
            "option_type": "call",
        }
    ]
    quote_rows = [
        {
            "instrument_name": "BTC-1JAN24-10000-C",
            "bid_price": 1.0,
            "volume": 12.0,
            "volume_usd": 34.0,
        }
    ]

    monkeypatch.setattr(option_chain_source, "deribit_rpc", _fake_rpc_factory(chain_rows=chain_rows, quote_rows=quote_rows))
    monkeypatch.setattr(option_chain_source, "DATA_ROOT", tmp_path)

    step_ts = 1_700_000_000_000
    root = tmp_path / "raw" / "option_chain"
    quote_root = tmp_path / "raw" / "option_quote"
    src = DeribitOptionChainRESTSource(
        currency="BTC",
        interval="1m",
        poll_interval_ms=60_000,
        root=root,
        quote_root=quote_root,
        chain_ttl_s=0,
    )

    try:
        src.fetch_step(step_ts=step_ts)
    finally:
        src._close_writers()

    quote_path = _date_path(quote_root, currency="BTC", interval="1m", step_ts=step_ts)
    df = pd.read_parquet(quote_path)
    assert "volume_24h" in df.columns
    assert "volume_usd_24h" in df.columns
    assert "volume" not in df.columns
    assert "fetch_step_ts" in df.columns
    assert "step_ts" not in df.columns
    assert "market_ts" in df.columns
    assert pd.isna(df.loc[0, "market_ts"])


def test_option_chain_rpc_left_join_preserves_chain_rows(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain_rows = [
        {
            "instrument_name": "BTC-1JAN24-10000-C",
            "expiration_timestamp": 1_700_100_000_000,
            "strike": 10_000,
            "option_type": "call",
        },
        {
            "instrument_name": "BTC-1JAN24-10000-P",
            "expiration_timestamp": 1_700_100_000_000,
            "strike": 10_000,
            "option_type": "put",
        },
    ]
    quote_rows = [
        {
            "instrument_name": "BTC-1JAN24-10000-C",
            "bid_price": 1.0,
        }
    ]

    monkeypatch.setattr(option_chain_source, "deribit_rpc", _fake_rpc_factory(chain_rows=chain_rows, quote_rows=quote_rows))
    monkeypatch.setattr(option_chain_source, "DATA_ROOT", tmp_path)

    step_ts = 1_700_000_000_000
    root = tmp_path / "raw" / "option_chain"
    quote_root = tmp_path / "raw" / "option_quote"
    src = DeribitOptionChainRESTSource(
        currency="BTC",
        interval="1m",
        poll_interval_ms=60_000,
        root=root,
        quote_root=quote_root,
        chain_ttl_s=0,
    )

    try:
        src.fetch_step(step_ts=step_ts)
    finally:
        src._close_writers()

    chain_path = _date_path(root, currency="BTC", interval="1m", step_ts=step_ts)
    df = pd.read_parquet(chain_path)
    assert set(df["instrument_name"].tolist()) == {"BTC-1JAN24-10000-C", "BTC-1JAN24-10000-P"}
    assert "fetch_step_ts" in df.columns
    assert "step_ts" not in df.columns
    missing_row = df[df["instrument_name"] == "BTC-1JAN24-10000-P"].iloc[0]
    assert pd.isna(missing_row["bid_price"])


def test_option_chain_rpc_path_partitioning(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chain_rows = [
        {
            "instrument_name": "BTC-1JAN24-10000-C",
            "expiration_timestamp": 1_700_100_000_000,
            "strike": 10_000,
            "option_type": "call",
        }
    ]
    quote_rows = [
        {
            "instrument_name": "BTC-1JAN24-10000-C",
            "bid_price": 1.0,
            "volume": 1.0,
            "volume_usd": 2.0,
        }
    ]

    monkeypatch.setattr(option_chain_source, "deribit_rpc", _fake_rpc_factory(chain_rows=chain_rows, quote_rows=quote_rows))
    monkeypatch.setattr(option_chain_source, "DATA_ROOT", tmp_path)

    step_ts = 1_700_000_000_000
    root = tmp_path / "raw" / "option_chain"
    quote_root = tmp_path / "raw" / "option_quote"
    src = DeribitOptionChainRESTSource(
        currency="BTC",
        interval="1m",
        poll_interval_ms=60_000,
        root=root,
        quote_root=quote_root,
        chain_ttl_s=0,
    )

    try:
        src.fetch_step(step_ts=step_ts)
    finally:
        src._close_writers()

    chain_path = _date_path(root, currency="BTC", interval="1m", step_ts=step_ts)
    quote_path = _date_path(quote_root, currency="BTC", interval="1m", step_ts=step_ts)
    assert chain_path.exists()
    assert quote_path.exists()
