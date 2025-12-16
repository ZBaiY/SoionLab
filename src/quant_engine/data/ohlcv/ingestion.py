from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

import requests

from quant_engine.data.ohlcv.cleaner import clean_ohlcv, OHLCVCleanConfig

# =========================
# Constants
# =========================

BINANCE_KLINES_ENDPOINT = "https://api.binance.com/api/v3/klines"

KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
] 

FLOAT_COLS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_asset_volume",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
]


# =========================
# Runtime replay handler
# =========================


@dataclass
class OHLCVIngestionEngine:
    """
    Offline-only OHLCV ingestion engine.

    Responsibilities:
    - Fetch OHLCV data from Binance in chunks
    - Clean raw data using OHLCVCleaner
    - Persist raw and cleaned parquet files partitioned by year

    MUST NOT be used at runtime or inside backtests.
    """
    data_root: Path
    limit: int = 1000
    timeout: int = 10
    clean_config: OHLCVCleanConfig = OHLCVCleanConfig()

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def _base_dir(self, kind: str, symbol: str, interval: str) -> Path:
        return self.data_root / "klines" / kind / symbol / interval

    def _year_path(self, kind: str, symbol: str, interval: str, year: int) -> Path:
        return self._base_dir(kind, symbol, interval) / f"{year}.parquet"

    # ------------------------------------------------------------------
    # Fetching (network)
    # ------------------------------------------------------------------

    def _fetch_chunk(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
    ) -> pd.DataFrame:
        r = requests.get(
            BINANCE_KLINES_ENDPOINT,
            params={
                "symbol": symbol,
                "interval": interval,
                "startTime": start_ms,
                "limit": self.limit,
            },
            timeout=self.timeout,
        )
        r.raise_for_status()
        data = r.json()

        if not data:
            return pd.DataFrame(columns=KLINE_COLUMNS)

        return pd.DataFrame(data, columns=KLINE_COLUMNS)

    def fetch_range(
        self,
        symbol: str,
        interval: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        cursor = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)

        frames: list[pd.DataFrame] = []

        while cursor < end_ms:
            df = self._fetch_chunk(symbol, interval, cursor)
            if df.empty:
                break
            frames.append(df)
            cursor = int(df.iloc[-1]["open_time"]) + 1

        if not frames:
            return pd.DataFrame(columns=KLINE_COLUMNS)

        return pd.concat(frames, ignore_index=True)

    # ------------------------------------------------------------------
    # Cleaning
    # ------------------------------------------------------------------

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean raw OHLCV data using OHLCVCleaner.
        """
        return clean_ohlcv(df, config=self.clean_config)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _write_parquet(
        self,
        df: pd.DataFrame,
        kind: str,
        symbol: str,
        interval: str,
    ) -> None:
        df = df.copy()
        df["year"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.year

        for year, chunk in df.groupby("year"):
            path = self._year_path(kind, symbol, interval, year)
            path.parent.mkdir(parents=True, exist_ok=True)
            chunk.to_parquet(path, engine="pyarrow", index=False)

    def fetch_and_store(
        self,
        symbol: str,
        interval: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        raw = self.fetch_range(symbol, interval, start, end)
        if raw.empty:
            return

        # persist raw
        self._write_parquet(raw, "raw", symbol, interval)

        # clean + persist cleaned
        cleaned = self.clean(raw)
        cleaned = cleaned.reset_index()  # open_time back to column for parquet partitioning
        self._write_parquet(cleaned, "cleaned", symbol, interval)
