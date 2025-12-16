from __future__ import annotations
from typing import List, Dict, Any, Optional
import pandas as pd

from quant_engine.utils.logger import get_logger, log_debug

_logger = get_logger(__name__)


class OHLCVLoader:
    """
    v4 OHLCV Loader (Canonical Normalizer)
    -------------------------------------

    This loader is a *pure schema normalizer*.

    Responsibilities:
    - accept OHLCV data from CSV / DataFrame / API dicts
    - resolve timestamp into UTC unix seconds
    - enforce column schema and ordering
    - return List[Dict[str, Any]] suitable for:
        - HistoricalOHLCVHandler
        - RealTimeDataHandler.on_new_tick

    Non-responsibilities:
    - no caching
    - no alignment
    - no windowing
    - no snapshot logic
    """

    REQUIRED_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]
    LEGACY_TIME_COLUMNS = ["open_time", "time", "ts"]

    # ------------------------------------------------------------------
    # Load from CSV
    # ------------------------------------------------------------------
    @classmethod
    def from_csv(cls, path: str, timezone: Optional[str] = None) -> List[Dict[str, Any]]:
        log_debug(_logger, "Loading OHLCV CSV", path=path)

        df = pd.read_csv(path)

        if timezone:
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(timezone)

        return cls._standardize(df)

    # ------------------------------------------------------------------
    # Load from DataFrame directly
    # ------------------------------------------------------------------
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List[Dict[str, Any]]:
        log_debug(_logger, "Loading OHLCV from DataFrame", rows=len(df))
        return cls._standardize(df)

    # ------------------------------------------------------------------
    # Load from a list of dict bars (API)
    # ------------------------------------------------------------------
    @classmethod
    def from_dict_list(cls, bars: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        log_debug(_logger, "Loading OHLCV from raw dict list", count=len(bars))
        df = pd.DataFrame(bars)
        return cls._standardize(df)

    # ------------------------------------------------------------------
    # Convert raw df into clean OHLCV list
    # ------------------------------------------------------------------
    @classmethod
    def _standardize(cls, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Canonicalize OHLCV schema.

        Output invariants:
        - timestamp: float (UTC unix seconds)
        - strictly increasing order
        - numeric OHLCV fields as float
        """

        df = df.copy()

        # ------------------------------------------------------------------
        # Resolve timestamp column
        # ------------------------------------------------------------------
        if "timestamp" not in df.columns:
            for col in cls.LEGACY_TIME_COLUMNS:
                if col in df.columns:
                    df["timestamp"] = df[col]
                    break
            else:
                raise ValueError(
                    "OHLCV loader requires a timestamp column "
                    "(timestamp | open_time | time | ts)"
                )

        # Normalize timestamp → UTC unix seconds (float)
        ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        if ts.isna().any():
            raise ValueError("Invalid timestamps detected during OHLCV loading")

        df["timestamp"] = ts.view("int64") / 1e9

        # ------------------------------------------------------------------
        # Validate required columns
        # ------------------------------------------------------------------
        missing = [c for c in cls.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Missing OHLCV columns: {missing}")

        # ------------------------------------------------------------------
        # Normalize numeric fields
        # ------------------------------------------------------------------
        numeric_cols = ["open", "high", "low", "close", "volume"]
        df[numeric_cols] = df[numeric_cols].astype(float)

        # ------------------------------------------------------------------
        # Enforce deterministic ordering
        # ------------------------------------------------------------------
        df = df.sort_values("timestamp").reset_index(drop=True)

        # Optional strict monotonicity check
        if not df["timestamp"].is_monotonic_increasing:
            raise ValueError("OHLCV timestamps are not monotonic")

        log_debug(_logger, "Standardized OHLCV", rows=len(df))

        raw = df[cls.REQUIRED_COLUMNS].to_dict(orient="records")

        return [{str(k): v for k, v in row.items()} for row in raw] 