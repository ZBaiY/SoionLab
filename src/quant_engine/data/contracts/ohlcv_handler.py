from __future__ import annotations
from typing import Protocol, Any
import pandas as pd



class OHLCVHandler(Protocol):
    """
    TradeBot v4 OHLCV Handler Protocol

    This protocol enforces the timestamp‑aligned data access rules used across:
        • Historical backtests
        • Mock trading (simulated streaming)
        • Live real‑time handlers

    All FeatureChannels **must** consume OHLCV data only through:
        • latest_bar()
        • get_snapshot(ts)
        • window(ts, n)

    v4 guarantees:
        - strict anti‑lookahead
        - deterministic reproducibility
        - unified behavior across backtest / mock / live modes
    """

    # ---------------------------
    # Metadata
    # ---------------------------
    @property
    def symbol(self) -> str:
        """Return symbol associated with this handler."""
        ...

    # ---------------------------
    # Required core API
    # ---------------------------
    def latest_bar(self) -> pd.DataFrame | dict:
        """
        Return the latest OHLCV bar.

        Expected format:
            - Single‑row DataFrame, OR
            - dict with keys ["open","high","low","close","volume"]
            
            Expected OHLCV bar format:
            {
                "timestamp": ...,
                "open": ...,
                "high": ...,
                "low": ...,
                "close": ...,
                "volume": ...
            }
        Strategy / Feature logic must treat this as a **single bar**.
        """
        ...

    def window_df(self, n: int) -> pd.DataFrame:
        """
        Return the last *n* bars as a DataFrame with shape (n, 5 or more).
        Columns typically include: ["open","high","low","close","volume"].
        """
        ...

    # ---------------------------
    # v4 timestamp‑aligned API
    # ---------------------------
    def get_snapshot(self, ts: float) -> Any:
        """
        Return the latest bar (or aggregated snapshot) with timestamp ≤ ts.
        Must enforce:
            • anti‑lookahead
            • reproducibility
            • deterministic ordering
        """
        ...

    def window(self, ts: float, n: int):
        """
        Return the most recent n bars where bar.timestamp ≤ ts.
        Used for all rolling feature computations (RSI, MACD, volatility, etc.).
        MUST NOT return bars with timestamp > ts.
        """
        ...

    def ready(self) -> bool:
        """
        Whether the handler has enough data to produce valid outputs.
        Real‑time handlers return True once at least one bar arrives.
        Historical handlers return True once data has loaded.
        """
        ...

    # ---------------------------
    # Optional convenience API
    # ---------------------------
    def last_timestamp(self) -> int | None:
        """
        Return the timestamp (float UNIX seconds) of the most recent bar.
        Used by the StrategyEngine/BacktestEngine to synchronize handlers.
        """
        return None

    def flush_cache(self) -> None:
        """Clear internal stored data (used by backtest resets)."""
        return None
