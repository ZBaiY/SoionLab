

from __future__ import annotations
from typing import List, Dict, Any, Optional
import pandas as pd

from quant_engine.utils.logger import get_logger, log_debug

_logger = get_logger(__name__)


class OrderbookLoader:
    """
    v4 Orderbook Loader
    -------------------
    A pure loader that:
        • loads raw L1/L2 orderbook snapshots
        • standardizes schema
        • returns list[dict] suitable for RealTimeOrderbookHandler or HistoricalOrderbookHandler

    It does NOT:
        • perform caching
        • perform alignment
        • create snapshots
    """

    REQUIRED_COLUMNS = ["timestamp", "best_bid", "best_ask", "bids", "asks"]

    # ---------------------------------------------------------------
    # Load from CSV
    # ---------------------------------------------------------------
    @classmethod
    def from_csv(cls, path: str) -> List[Dict[str, Any]]:
        log_debug(_logger, "Loading orderbook CSV", path=path)
        df = pd.read_csv(path)
        return cls._standardize(df)

    # ---------------------------------------------------------------
    # Load from DataFrame
    # ---------------------------------------------------------------
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List[Dict[str, Any]]:
        log_debug(_logger, "Loading orderbook DataFrame", rows=len(df))
        return cls._standardize(df)

    # ---------------------------------------------------------------
    # Load from raw API dict list
    # ---------------------------------------------------------------
    @classmethod
    def from_dict_list(cls, snapshots: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        log_debug(_logger, "Loading orderbook dict-list", count=len(snapshots))
        df = pd.DataFrame(snapshots)
        return cls._standardize(df)

    # ---------------------------------------------------------------
    # Standardization
    # ---------------------------------------------------------------
    @classmethod
    def _standardize(cls, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Convert raw orderbook rows into a uniform list[Dict[str, Any]].
        - timestamp → float (seconds)
        - best_bid / best_ask → float
        - bids / asks → ensure list[dict]
        """

        missing = [c for c in cls.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Missing orderbook columns: {missing}")

        df = df.copy()

        df["timestamp"] = df["timestamp"].astype(float)
        df["best_bid"] = df["best_bid"].astype(float)
        df["best_ask"] = df["best_ask"].astype(float)

        # Ensure bids/asks are list[dict]
        def ensure_list(x):
            if isinstance(x, list):
                return x
            try:
                return eval(x)  # fallback for CSV stringified lists
            except Exception:
                return []

        df["bids"] = df["bids"].apply(ensure_list)
        df["asks"] = df["asks"].apply(ensure_list)

        df = df.sort_values("timestamp")

        log_debug(_logger, "Standardized orderbook", rows=len(df))

        raw = df.to_dict(orient="records")

        cleaned: List[Dict[str, Any]] = [
            {str(k): v for k, v in row.items()} for row in raw
        ]

        return cleaned