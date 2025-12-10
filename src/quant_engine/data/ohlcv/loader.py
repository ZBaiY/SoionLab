from __future__ import annotations
from typing import List, Dict, Any, Optional
import pandas as pd

from quant_engine.utils.logger import get_logger, log_debug

_logger = get_logger(__name__)


class OHLCVLoader:
    """
    v4 OHLCV Loader
    ----------------
    This loader is a *pure data constructor*:
        - loads OHLCV data from CSV / DataFrame / API-compatible dicts
        - standardizes the schema
        - returns a list of OHLCV bars (each as a dict)
    
    It performs **no caching**, **no alignment**, and **no snapshot logic**.
    Those responsibilities belong to:
        - RealTimeDataHandler
        - HistoricalDataHandler
        - OHLCVCache
    """

    REQUIRED_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]

    # ------------------------------------------------------------------
    # Load from CSV
    # ------------------------------------------------------------------
    @classmethod
    def from_csv(cls, path: str, timezone: Optional[str] = None) -> List[Dict[str, Any]]:
        log_debug(_logger, "Loading OHLCV CSV", path=path)

        df = pd.read_csv(path)

        if timezone:
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(timezone).astype("int64") / 1e9
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"]).astype("int64") / 1e9

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
        Ensure the OHLCV data matches the required column & type schema.
        Produce list[dict] that can be fed into HistoricalDataHandler.
        """

        missing = [c for c in cls.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Missing OHLCV columns: {missing}")

        df = df.copy()

        # Convert to float for numerical stability
        numeric_cols = ["open", "high", "low", "close", "volume"]
        df[numeric_cols] = df[numeric_cols].astype(float)

        # Convert timestamp → float (UNIX seconds)
        df["timestamp"] = df["timestamp"].astype(float)

        # Sort by timestamp to guarantee determinism
        df = df.sort_values("timestamp")

        log_debug(_logger, "Standardized OHLCV", rows=len(df))

        raw = df.to_dict(orient="records")

        # Enforce Dict[str, Any] for type checkers (Pylance, MyPy)
        cleaned: List[Dict[str, Any]] = [
            {str(k): v for k, v in row.items()}
            for row in raw
        ]

        return cleaned