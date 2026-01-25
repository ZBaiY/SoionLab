from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import ingestion.ohlcv.source as ohlcv_source
from ingestion.contracts.tick import IngestionTick, _to_interval_ms
from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer
from ingestion.ohlcv.source import OHLCVFileSource
from ingestion.ohlcv.worker import OHLCVWorker


def _write_ohlcv_parquet(
    root: Path,
    *,
    symbol: str,
    interval: str,
    rows: list[dict[str, Any]],
) -> Path:
    base = root / symbol / interval
    base.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    path = base / "2024.parquet"
    df.to_parquet(path, index=False)
    return path


@pytest.mark.asyncio
async def test_tick_identity_preservation_async_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ohlcv_source, "DATA_ROOT", tmp_path)
    root = tmp_path / "cleaned" / "ohlcv"
    interval = "1m"
    interval_ms = _to_interval_ms(interval)
    assert interval_ms is not None

    base_ts = 1_700_000_000_000
    btc_rows = [
        {
            "data_ts": base_ts,
            "open_time": base_ts - int(interval_ms),
            "close_time": base_ts,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10.0,
            "quote_asset_volume": 111,
        },
        {
            "data_ts": base_ts + 120_000,
            "open_time": base_ts + 60_000,
            "close_time": base_ts + 120_000,
            "open": 2.0,
            "high": 3.0,
            "low": 1.5,
            "close": 2.5,
            "volume": 12.0,
            "quote_asset_volume": 111,
        },
    ]
    eth_rows = [
        {
            "data_ts": base_ts + 60_000,
            "open_time": base_ts,
            "close_time": base_ts + 60_000,
            "open": 10.0,
            "high": 11.0,
            "low": 9.5,
            "close": 10.5,
            "volume": 20.0,
            "quote_asset_volume": 222,
        },
        {
            "data_ts": base_ts + 180_000,
            "open_time": base_ts + 120_000,
            "close_time": base_ts + 180_000,
            "open": 11.0,
            "high": 12.0,
            "low": 10.5,
            "close": 11.5,
            "volume": 22.0,
            "quote_asset_volume": 222,
        },
    ]

    _write_ohlcv_parquet(root, symbol="BTCUSDT", interval=interval, rows=btc_rows)
    _write_ohlcv_parquet(root, symbol="ETHUSDT", interval=interval, rows=eth_rows)

    source_btc = OHLCVFileSource(root="cleaned/ohlcv", symbol="BTCUSDT", interval=interval)
    source_eth = OHLCVFileSource(root="cleaned/ohlcv", symbol="ETHUSDT", interval=interval)
    worker_btc = OHLCVWorker(
        normalizer=BinanceOHLCVNormalizer(symbol="BTCUSDT"),
        source=source_btc,
        symbol="BTCUSDT",
        interval=interval,
        interval_ms=int(interval_ms),
    )
    worker_eth = OHLCVWorker(
        normalizer=BinanceOHLCVNormalizer(symbol="ETHUSDT"),
        source=source_eth,
        symbol="ETHUSDT",
        interval=interval,
        interval_ms=int(interval_ms),
    )

    tick_queue: asyncio.PriorityQueue[tuple[int, int, IngestionTick]] = asyncio.PriorityQueue()
    emitted: list[IngestionTick] = []
    seq = 0

    async def emit_to_queue(tick: IngestionTick) -> None:
        nonlocal seq
        emitted.append(tick)
        seq_key = seq
        seq += 1
        await tick_queue.put((int(tick.data_ts), -seq_key, tick))

    await asyncio.gather(worker_btc.run(emit_to_queue), worker_eth.run(emit_to_queue))

    seen: list[tuple[str, str, Any]] = []
    seen_ids: list[int] = []
    seen_sources: list[str | None] = []
    while True:
        try:
            _, _, tick = tick_queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        seen_ids.append(id(tick))
        seen_sources.append(getattr(tick, "source_id", None))
        seen.append((tick.domain, tick.symbol, tick.payload.get("quote_asset_volume")))

    assert len(seen_ids) == len(set(seen_ids))
    expected = {"BTCUSDT": 111, "ETHUSDT": 222}
    for domain, symbol, sentinel in seen:
        assert domain == "ohlcv"
        assert symbol in expected
        assert sentinel == expected[symbol]

    symbols = [s for _, s, _ in seen]
    assert symbols.count("BTCUSDT") == 2
    assert symbols.count("ETHUSDT") == 2
    assert set(seen_sources) == {worker_btc._source_id, worker_eth._source_id}
