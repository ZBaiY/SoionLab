from __future__ import annotations

import json  # +
from datetime import datetime, timezone

import pandas as pd
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

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


def test_pack_aux_deterministic_key_order() -> None:  # +
    df = pd.DataFrame([{"instrument_name": "BTC-1", "aux": {"z": 0}, "strike": 100.0, "expiry_ms": 123}])  # +
    cols_to_aux = {"strike", "expiry_ms"}  # +
    out_a = option_chain_source._pack_aux(df, cols_to_aux)  # +
    out_b = option_chain_source._pack_aux(df, cols_to_aux)  # +
    keys_a = list(out_a.loc[0, "aux"].keys())  # +
    keys_b = list(out_b.loc[0, "aux"].keys())  # +
    assert keys_a == keys_b  # +
    dump_a = json.dumps(out_a.loc[0, "aux"], sort_keys=False, separators=(",", ":"))  # +
    dump_b = json.dumps(out_b.loc[0, "aux"], sort_keys=False, separators=(",", ":"))  # +
    assert dump_a == dump_b  # +


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
    universe_root = tmp_path / "raw" / "option_universe"
    src = DeribitOptionChainRESTSource(
        currency="BTC",
        interval="1m",
        poll_interval_ms=60_000,
        root=root,
        quote_root=quote_root,
        universe_root=universe_root,
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
    assert str(df["market_ts"].dtype) == "Int64"
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
    universe_root = tmp_path / "raw" / "option_universe"
    src = DeribitOptionChainRESTSource(
        currency="BTC",
        interval="1m",
        poll_interval_ms=60_000,
        root=root,
        quote_root=quote_root,
        universe_root=universe_root,
        chain_ttl_s=0,
    )

    try:
        payloads = src.fetch_step(step_ts=step_ts)
    finally:
        src._close_writers()

    assert payloads is not None and len(payloads) == 1
    data_ts = int(payloads[0]["data_ts"])
    chain_path = _date_path(root, currency="BTC", interval="1m", step_ts=data_ts)
    df = pd.read_parquet(chain_path)
    assert set(df["instrument_name"].tolist()) == {"BTC-1JAN24-10000-C", "BTC-1JAN24-10000-P"}
    assert "arrival_ts" in df.columns
    assert "data_ts" not in df.columns
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
    universe_root = tmp_path / "raw" / "option_universe"
    src = DeribitOptionChainRESTSource(
        currency="BTC",
        interval="1m",
        poll_interval_ms=60_000,
        root=root,
        quote_root=quote_root,
        universe_root=universe_root,
        chain_ttl_s=0,
    )

    try:
        payloads = src.fetch_step(step_ts=step_ts)
    finally:
        src._close_writers()

    assert payloads is not None and len(payloads) == 1
    data_ts = int(payloads[0]["data_ts"])
    chain_path = _date_path(root, currency="BTC", interval="1m", step_ts=data_ts)
    quote_path = _date_path(quote_root, currency="BTC", interval="1m", step_ts=step_ts)
    dt = datetime.fromtimestamp(int(data_ts) / 1000.0, tz=timezone.utc)
    universe_path = tmp_path / "raw" / "option_universe" / "BTC" / dt.strftime("%Y") / f"{dt.strftime('%Y_%m_%d')}.parquet"
    assert chain_path.exists()
    assert quote_path.exists()
    assert universe_path.exists()


def test_option_chain_rpc_fetch_step_without_records_still_writes(  # +
    tmp_path,  # +
    monkeypatch: pytest.MonkeyPatch,  # +
) -> None:  # +
    chain_rows = [  # +
        {  # +
            "instrument_name": "BTC-1JAN24-10000-C",  # +
            "expiration_timestamp": 1_700_100_000_000,  # +
            "strike": 10_000,  # +
            "option_type": "call",  # +
        },  # +
        {  # +
            "instrument_name": "BTC-1JAN24-10000-P",  # +
            "expiration_timestamp": 1_700_100_000_000,  # +
            "strike": 10_000,  # +
            "option_type": "put",  # +
        },  # +
    ]  # +
    quote_rows = [  # +
        {"instrument_name": "BTC-1JAN24-10000-C", "bid_price": 1.0},  # +
    ]  # +
    monkeypatch.setattr(option_chain_source, "deribit_rpc", _fake_rpc_factory(chain_rows=chain_rows, quote_rows=quote_rows))  # +
    monkeypatch.setattr(option_chain_source, "DATA_ROOT", tmp_path)  # +
    step_ts = 1_700_000_000_000  # +
    root = tmp_path / "raw" / "option_chain"  # +
    quote_root = tmp_path / "raw" / "option_quote"  # +
    universe_root = tmp_path / "raw" / "option_universe"  # +
    src = DeribitOptionChainRESTSource(  # +
        currency="BTC",  # +
        interval="1m",  # +
        poll_interval_ms=60_000,  # +
        root=root,  # +
        quote_root=quote_root,  # +
        universe_root=universe_root,  # +
        chain_ttl_s=0,  # +
    )  # +
    try:  # +
        payloads = src.fetch_step(step_ts=step_ts, include_records=False)  # +
    finally:  # +
        src._close_writers()  # +
    assert payloads is not None and len(payloads) == 1  # +
    assert payloads[0]["records"] is None  # +
    data_ts = int(payloads[0]["data_ts"])  # +
    chain_path = _date_path(root, currency="BTC", interval="1m", step_ts=data_ts)  # +
    assert chain_path.exists()  # +
    df = pd.read_parquet(chain_path)  # +
    assert len(df) == 2  # +


def test_option_chain_writer_init_creates_missing_partition_dirs(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(option_chain_source, "DATA_ROOT", tmp_path)
    root = tmp_path / "raw" / "option_chain"
    quote_root = tmp_path / "raw" / "option_quote"
    universe_root = tmp_path / "raw" / "option_universe"
    src = DeribitOptionChainRESTSource(
        currency="BTC",
        interval="1m",
        poll_interval_ms=60_000,
        root=root,
        quote_root=quote_root,
        universe_root=universe_root,
        chain_ttl_s=0,
    )
    path = quote_root / "BTC" / "1m" / "2099" / "2099_01_01.parquet"
    assert not path.parent.exists()
    df = pd.DataFrame([{"instrument_name": "BTC-1JAN24-10000-C", "bid_price": 1.0, "fetch_step_ts": 1_700_000_000_000}])
    try:
        writer, schema = src._get_writer_and_schema(path, df)
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        writer.write_table(table)
    finally:
        src._close_writers()
    assert path.exists()
    table = pq.read_table(path)
    assert table.num_rows == 1
