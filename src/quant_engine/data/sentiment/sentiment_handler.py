from __future__ import annotations

from typing import Any

import pandas as pd

from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler
from quant_engine.data.contracts.snapshot import (
    MarketSpec,
    ensure_market_spec,
    merge_market_spec,
    classify_gap,
)
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
    market: MarketSpec
    gap_min_gap_ms: int | None
    _backfill_fn: Any | None

    bootstrap_cfg: dict[str, Any] | None
    _anchor_ts: int | None
    _logger: Any

    def __init__(self, symbol: str, **kwargs: Any):
        self.symbol = symbol

        cache_cfg = kwargs.get("cache") or {}
        if not isinstance(cache_cfg, dict):
            raise TypeError("Sentiment 'cache' must be a dict")

        self.bootstrap_cfg = kwargs.get("bootstrap") or None
    
        maxlen = int(cache_cfg.get("maxlen", kwargs.get("maxlen", 5_000)))
        if maxlen <= 0:
            raise ValueError("Sentiment cache.maxlen must be > 0")

        self.cache = SentimentDataCache(maxlen=maxlen)
        self.market = ensure_market_spec(
            kwargs.get("market"),
            default_venue=str(kwargs.get("venue", kwargs.get("source", "unknown"))),
            default_asset_class=str(kwargs.get("asset_class", "crypto")),
            default_timezone=str(kwargs.get("timezone", "UTC")),
            default_calendar=str(kwargs.get("calendar", "24x7")),
            default_session=str(kwargs.get("session", "24x7")),
            default_currency=kwargs.get("currency"),
        )
        gap_cfg = kwargs.get("gap") or {}
        if not isinstance(gap_cfg, dict):
            raise TypeError("Sentiment 'gap' must be a dict")
        min_gap_ms = gap_cfg.get("min_gap_ms")
        self.gap_min_gap_ms = int(min_gap_ms) if min_gap_ms is not None else None
        self._backfill_fn = kwargs.get("backfill_fn") or kwargs.get("backfill")

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
        self._maybe_backfill(anchor_ts=anchor_ts, lookback=lookback)

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

            payload = row.to_dict()
            last = self.cache.last()
            last_ts = int(last.data_ts) if last is not None else None
            ts = payload.get("data_ts")
            assert ts is not None, "Sentiment event must contain 'data_ts'"
            market = _resolve_market(
                self.market,
                payload,
                last_ts=last_ts,
                data_ts=int(ts),
                min_gap_ms=self.gap_min_gap_ms,
            )
            snap = SentimentSnapshot.from_event_aligned(
                timestamp = self._anchor_ts,
                event=payload,
                symbol=self.symbol,
                market=market,
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

    def _maybe_backfill(self, *, anchor_ts: int | None, lookback: Any | None) -> None:
        if self._backfill_fn is None or anchor_ts is None:
            return
        window_ms = _coerce_lookback_ms(lookback, None)
        if window_ms is None:
            return
        start_ts = int(anchor_ts) - int(window_ms)
        if self.cache.get_at_or_before(start_ts) is not None:
            return
        prev_anchor = self._anchor_ts
        if prev_anchor is None:
            self._anchor_ts = int(anchor_ts)
        try:
            for row in self._backfill_fn(start_ts=int(start_ts), end_ts=int(anchor_ts)):
                self.on_new_tick(row)
        finally:
            if prev_anchor is None:
                self._anchor_ts = prev_anchor


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


def _resolve_market(
    base: MarketSpec,
    payload: dict[str, Any],
    *,
    last_ts: int | None,
    data_ts: int | None,
    min_gap_ms: int | None,
) -> MarketSpec:
    market_payload = payload.get("market")
    market_status = None
    if isinstance(market_payload, dict):
        market_status = market_payload.get("status")
    status = payload.get("status", market_status)
    override = payload.get("market")
    if isinstance(override, dict):
        override = dict(override)
        override.pop("gap_type", None)
    gap_type = classify_gap(
        status=status,
        last_ts=last_ts,
        data_ts=data_ts,
        expected_interval_ms=None,
        min_gap_ms=min_gap_ms,
    )
    return merge_market_spec(base, override, status=status, gap_type=gap_type)


def _coerce_lookback_ms(lookback: Any, interval_ms: int | None) -> int | None:
    if lookback is None:
        return None
    if isinstance(lookback, dict):
        window_ms = lookback.get("window_ms")
        if window_ms is not None:
            return int(window_ms)
        return None
    if isinstance(lookback, (int, float)):
        return int(float(lookback))
    return None
