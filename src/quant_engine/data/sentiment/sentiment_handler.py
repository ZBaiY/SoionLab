from __future__ import annotations

from typing import Any

import pandas as pd

from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler
from quant_engine.utils.logger import get_logger, log_debug, log_info

from .cache import SentimentDataCache
from .snapshot import SentimentSnapshot


class SentimentDataHandler(RealTimeDataHandler):
    """
    Runtime Sentiment handler (mode-agnostic).
    """

    symbol: str
    cache: SentimentDataCache
    columns: list[str] | None

    _anchor_ts: int | None
    _logger: Any

    def __init__(self, symbol: str, **kwargs: Any):
        self.symbol = symbol

        cache_cfg = kwargs.get("cache") or {}
        if not isinstance(cache_cfg, dict):
            raise TypeError("Sentiment 'cache' must be a dict")

        maxlen = int(cache_cfg.get("maxlen", kwargs.get("maxlen", 5_000)))
        if maxlen <= 0:
            raise ValueError("Sentiment cache.maxlen must be > 0")

        self.cache = SentimentDataCache(maxlen=maxlen)

        # DataFrame view columns (legacy / research convenience)
        self.columns = kwargs.get(
            "columns",
            ["data_ts", "score", "source"],
        )

        self._anchor_ts = None
        self._logger = get_logger(__name__)

        log_debug(
            self._logger,
            "SentimentDataHandler initialized",
            symbol=self.symbol,
            maxlen=maxlen,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def align_to(self, ts: int) -> None:
        """Clamp implicit reads to ts (anti-lookahead anchor)."""
        self._anchor_ts = int(ts)
        log_debug(
            self._logger,
            "SentimentDataHandler align_to",
            symbol=self.symbol,
            anchor_ts=self._anchor_ts,
        )

    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        """No-op bootstrap (IO-free)."""
        log_debug(
            self._logger,
            "SentimentDataHandler.bootstrap (no-op)",
            symbol=self.symbol,
            anchor_ts=anchor_ts,
            lookback=lookback,
        )

    # ------------------------------------------------------------------
    # Streaming tick API
    # ------------------------------------------------------------------

    def on_new_tick(self, event: Any) -> None:
        """
        Ingest sentiment payload(s).

        Accepted inputs:
          - dict (single event)
          - list[dict] / iterable
          - DataFrame
        """
        df = _coerce_sentiment_to_df(event)
        if df is None or df.empty:
            return

        if "data_ts" not in df.columns:
            raise KeyError("Sentiment payload must contain 'data_ts'")

        df = df.sort_values("data_ts")

        for _, row in df.iterrows():
            if self._anchor_ts is None:
                raise RuntimeError("SentimentDataHandler.on_new_tick called before align_to()")

            snap = SentimentSnapshot.from_event_aligned(
                timestamp = self._anchor_ts,
                event=row.to_dict(),
                symbol=self.symbol,
            )
            self.cache.push(snap)

    # ------------------------------------------------------------------
    # Unified access
    # ------------------------------------------------------------------

    def last_timestamp(self) -> int | None:
        snap = self.cache.last()
        if snap is None:
            return None

        ts = snap.data_ts
        if self._anchor_ts is not None:
            return min(ts, self._anchor_ts)
        return ts

    def get_snapshot(self, ts: int | None = None) -> SentimentSnapshot | None:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None

        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        return self.cache.get_at_or_before(t)

    def window(self, ts: int | None = None, n: int = 1) -> pd.DataFrame:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return pd.DataFrame()

        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        snaps = self.cache.get_n_before(t, n)

        if not snaps:
            return pd.DataFrame()

        assert self.columns is not None
        rows = [s.to_dict_col(self.columns) for s in snaps]
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Legacy
    # ------------------------------------------------------------------

    def window_df(self, window: int | None = None) -> pd.DataFrame:
        return self.window(n=window) if window is not None else self.window()

    def reset(self) -> None:
        log_info(self._logger, "SentimentDataHandler reset requested", symbol=self.symbol)
        self.cache.clear()


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _coerce_sentiment_to_df(x: Any) -> pd.DataFrame | None:
    if x is None:
        return None

    if isinstance(x, pd.DataFrame):
        return x

    if isinstance(x, dict):
        return pd.DataFrame([x])

    try:
        df = pd.DataFrame(x)
    except Exception:
        return None

    return df
