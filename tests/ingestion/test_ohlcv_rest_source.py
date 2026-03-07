from __future__ import annotations

import threading
from typing import Any
import types
import logging
from datetime import datetime, timezone

import pytest
import requests
import pyarrow.parquet as pq

import ingestion.ohlcv.source as ohlcv_source
from ingestion.contracts.tick import IngestionTick
from ingestion.ohlcv.source import (
    BinanceKlinesRESTSource,
    OHLCVRESTSource,
    make_binance_kline_fetch_fn,
)


def _response(payload: Any) -> Any:
    return types.SimpleNamespace(
        raise_for_status=lambda: None,
        json=lambda: payload,
    )


def test_make_binance_kline_fetch_fn_sets_data_ts_and_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = [
        [1000, "1", "2", "0.5", "1.5", "10", 1500, "0", 1, "0", "0", "0"],
        [2000, "2", "3", "1.5", "2.5", "11", 2500, "0", 2, "0", "0", "0"],
    ]

    def fake_get(url: str, params: dict[str, Any], timeout: float) -> Any:
        assert params["symbol"] == "BTCUSDT"
        assert params["interval"] == "1m"
        return _response(payload)

    monkeypatch.setattr(requests, "get", fake_get)

    fetch_fn = make_binance_kline_fetch_fn(symbol="BTCUSDT", interval="1m", limit=2)
    rows = list(fetch_fn())

    assert [r["close_time"] for r in rows] == [1500, 2500]
    assert all(r["data_ts"] == r["close_time"] for r in rows)
    assert [r["data_ts"] for r in rows] == sorted(r["data_ts"] for r in rows)
    assert all(not isinstance(r, IngestionTick) for r in rows)


def test_ohlcv_rest_source_iteration_order_and_poll_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [{"data_ts": 1000}, {"data_ts": 2000}]
    stop_event = threading.Event()
    waits: list[float] = []

    def fake_wait(seconds: float) -> bool:
        waits.append(seconds)
        return True

    monkeypatch.setattr(stop_event, "wait", fake_wait)

    def fetch_fn() -> list[dict[str, int]]:
        return list(rows)

    src = OHLCVRESTSource(fetch_fn=fetch_fn, poll_interval_ms=250, stop_event=stop_event)
    assert list(src.fetch()) == rows
    out = list(src)

    assert out == rows
    assert [r["data_ts"] for r in out] == [1000, 2000]
    assert waits == [0.25]


def test_ohlcv_rest_source_backfill_delegates() -> None:
    calls: list[tuple[int, int]] = []

    def backfill_fn(*, start_ts: int, end_ts: int) -> list[dict[str, int]]:
        calls.append((start_ts, end_ts))
        return [{"data_ts": start_ts}, {"data_ts": end_ts}]

    src = OHLCVRESTSource(fetch_fn=lambda: [], backfill_fn=backfill_fn, poll_interval_ms=1000)
    rows = list(src.backfill(start_ts=10, end_ts=20))

    assert calls == [(10, 20)]
    assert rows[0]["data_ts"] == 10
    assert rows[1]["data_ts"] == 20


def test_binance_rest_source_emits_only_closed_bar(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    payload = [
        [1000, "1", "2", "0.5", "1.5", "10", 1500, "0", 1, "0", "0", "0"],
        [2000, "2", "3", "1.5", "2.5", "11", 2500, "0", 2, "0", "0", "0"],
    ]

    def fake_get(url: str, params: dict[str, Any], timeout: float) -> Any:
        return _response(payload)

    stop_event = threading.Event()

    def fake_wait(seconds: float) -> bool:
        return True

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(stop_event, "wait", fake_wait)
    monkeypatch.setattr(ohlcv_source, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ohlcv_source, "_RAW_OHLCV_ROOT", tmp_path / "raw" / "ohlcv")
    src = BinanceKlinesRESTSource(symbol="BTCUSDT", interval="1m", root=tmp_path / "raw" / "ohlcv", stop_event=stop_event)
    monkeypatch.setattr(src, "_write_raw_snapshot", lambda *_args, **_kwargs: None)

    rows = list(src)
    
    assert len(rows) == 1
    assert rows[0]["close_time"] == 1500
    assert rows[0]["data_ts"] == 1500
    assert not isinstance(rows[0], IngestionTick)

def test_binance_rest_source_skips_open_bar(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    payload = [
        [1000, "1", "2", "0.5", "1.5", "10", 5_000, "0", 1, "0", "0", "0"],
    ]

    def fake_get(url: str, params: dict[str, Any], timeout: float) -> Any:
        return _response(payload)

    stop_event = threading.Event()

    def fake_wait(seconds: float) -> bool:
        return True

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(stop_event, "wait", fake_wait)
    monkeypatch.setattr(ohlcv_source, "_now_ms", lambda: 4_000)
    monkeypatch.setattr(ohlcv_source, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ohlcv_source, "_RAW_OHLCV_ROOT", tmp_path / "raw" / "ohlcv")

    src = BinanceKlinesRESTSource(symbol="BTCUSDT", interval="1m", root=tmp_path / "raw" / "ohlcv", stop_event=stop_event)
    monkeypatch.setattr(src, "_write_raw_snapshot", lambda *_args, **_kwargs: None)

    rows = list(src)
    assert rows == []

def test_binance_rest_source_now_1600_filters_open_bar(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    # Simulate "now" = 1600ms
    monkeypatch.setattr(ohlcv_source, "_now_ms", lambda: 1600)

    # API returns ONLY 1 bar; this forces the len(rows)==1 branch,
    # which uses close_time vs now to decide closed/open.
    payload = [
        [1000, "1", "2", "0.5", "1.5", "10", 1600, "0", 1, "0", "0", "0"],  # close_time == now => open/unsafe
    ]

    def fake_get(url: str, params: dict[str, Any], timeout: float) -> Any:
        return _response(payload)

    stop_event = threading.Event()

    # Stop after first loop iteration (after sleep)
    monkeypatch.setattr(stop_event, "wait", lambda _seconds: True)
    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(ohlcv_source, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(ohlcv_source, "_RAW_OHLCV_ROOT", tmp_path / "raw" / "ohlcv")

    src = BinanceKlinesRESTSource(
        symbol="BTCUSDT",
        interval="1m",
        root=tmp_path / "raw" / "ohlcv",  # avoid resolve_under_root failure
        stop_event=stop_event,
    )
    monkeypatch.setattr(src, "_write_raw_snapshot", lambda *_args, **_kwargs: None)

    rows = list(src)
    
    assert rows == []  # open bar filtered because close_time >= now


def test_binance_rest_source_fetch_logs_throttled_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    src = BinanceKlinesRESTSource(symbol="BTCUSDT", interval="1m")
    monkeypatch.setattr(ohlcv_source, "_binance_klines_rest", lambda **_kwargs: (_ for _ in ()).throw(requests.Timeout("boom")))

    with caplog.at_level(logging.WARNING):
        with pytest.raises(requests.Timeout):
            src.fetch()
        with pytest.raises(requests.Timeout):
            src.fetch()

    logs = [rec for rec in caplog.records if "ohlcv.rest.fetch_error" in rec.getMessage()]
    assert len(logs) == 1


def test_write_raw_snapshot_quarantines_corrupt_parquet_and_continues(tmp_path) -> None:
    root = tmp_path / "raw" / "ohlcv"
    year_path = root / "BTCUSDT" / "15m" / "2026.parquet"
    year_path.parent.mkdir(parents=True, exist_ok=True)
    year_path.write_bytes(b"not-a-parquet")

    data_ts = int(datetime(2026, 3, 6, tzinfo=timezone.utc).timestamp() * 1000)
    bar = {
        "open_time": data_ts - 900_000,
        "close_time": data_ts,
        "data_ts": data_ts,
        "open": "1",
        "high": "2",
        "low": "0.5",
        "close": "1.5",
        "volume": "10",
        "quote_asset_volume": "0",
        "number_of_trades": 1,
        "taker_buy_base_asset_volume": "0",
        "taker_buy_quote_asset_volume": "0",
        "ignore": "0",
    }
    used_paths: set[Any] = set()
    ohlcv_source._write_raw_snapshot(
        root=root,
        symbol="BTCUSDT",
        interval="15m",
        bar=bar,
        used_paths=used_paths,
    )
    ohlcv_source._close_used_paths(used_paths)

    table = pq.read_table(year_path)
    assert table.num_rows == 1
    assert table.column("data_ts").to_pylist() == [data_ts]
    quarantined = list(year_path.parent.glob("2026.parquet.bak.corrupt.*"))
    assert quarantined
