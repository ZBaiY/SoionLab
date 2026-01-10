from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.runtime.modes import EngineMode


def _write_ohlcv_parquet(
    root: Path,
    *,
    symbol: str,
    interval: str,
    rows: list[dict[str, Any]],
) -> Path:
    base = root / symbol / interval
    base.mkdir(parents=True, exist_ok=True)
    path = base / "2024.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def test_cross_source_isolation(tmp_path: Path) -> None:
    interval = "1m"
    base_ts = 1_704_067_200_000

    data_root_a = tmp_path / "root_a"
    data_root_b = tmp_path / "root_b"
    root_a = data_root_a / "cleaned" / "ohlcv"
    root_b = data_root_b / "cleaned" / "ohlcv"

    _write_ohlcv_parquet(
        root_a,
        symbol="BTCUSDT",
        interval=interval,
        rows=[
            {
                "data_ts": base_ts,
                "open_time": base_ts - 60_000,
                "close_time": base_ts,
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10.0,
                "quote_asset_volume": 111,
            }
        ],
    )
    _write_ohlcv_parquet(
        root_b,
        symbol="BTCUSDT",
        interval=interval,
        rows=[
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
            }
        ],
    )

    handler_a = OHLCVDataHandler(
        "BTCUSDT",
        interval=interval,
        mode=EngineMode.MOCK,
        data_root=data_root_a,
        source_id=data_root_a,
    )
    handler_b = OHLCVDataHandler(
        "BTCUSDT",
        interval=interval,
        mode=EngineMode.MOCK,
        data_root=data_root_b,
        source_id=data_root_b,
    )

    handler_a.bootstrap(anchor_ts=base_ts + 60_000, lookback={"bars": 2})
    handler_b.bootstrap(anchor_ts=base_ts + 120_000, lookback={"bars": 2})

    snap_a = handler_a.cache.last()
    snap_b = handler_b.cache.last()
    assert snap_a is not None
    assert snap_b is not None
    assert snap_a.aux.get("quote_asset_volume") == 111
    assert snap_b.aux.get("quote_asset_volume") == 222
