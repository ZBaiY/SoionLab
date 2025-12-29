from __future__ import annotations

import time
from typing import Any

from pyparsing import Iterable, deque
from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler, to_interval_ms
from quant_engine.data.contracts.snapshot import (
    MarketSpec,
    ensure_market_spec,
    merge_market_spec,
    classify_gap,
)
from quant_engine.utils.logger import get_logger, log_debug, log_info

from quant_engine.data.orderbook.cache import OrderbookCache
from quant_engine.data.orderbook.snapshot import OrderbookSnapshot


class RealTimeOrderbookHandler(RealTimeDataHandler):
    """Runtime orderbook handler (mode-agnostic).

    Conforms to runtime handler protocol semantics:
      - kwargs-driven __init__ (loader passes nested handler config via **cfg)
      - bootstrap(end_ts, lookback) present (no-op by default; IO-free runtime)
      - warmup_to(ts) establishes anti-lookahead anchor
      - get_snapshot(ts=None) / window(ts=None,n) are timestamp-aligned
      - BACKTEST seeding via from_historical(...) + driver replay into on_new_tick(...)

    Note: This handler stores OrderbookSnapshot objects (not DataFrames).
    """

    # --- declared attributes (protocol/typing) ---
    symbol: str
    interval: str | None
    interval_ms: int | None
    bootstrap_cfg: dict[str, Any]
    cache_cfg: dict[str, Any]
    cache: OrderbookCache
    market: MarketSpec
    gap_min_gap_ms: int | None
    _backfill_fn: Any | None
    _anchor_ts: int | None
    _logger: Any

    def __init__(self, symbol: str, **kwargs: Any):
        self.symbol = symbol

        ri = kwargs.get("interval")
        if ri is not None and (not isinstance(ri, str) or not ri):
            raise ValueError("Orderbook 'interval' must be a non-empty string if provided")
        self.interval = ri
        interval_ms = to_interval_ms(self.interval) if self.interval is not None else None
        if self.interval is not None and interval_ms is None:
            raise ValueError(f"Invalid interval format: {self.interval}")
        self.interval_ms = int(interval_ms) if interval_ms is not None else None

        # Optional nested configs
        bootstrap = kwargs.get("bootstrap") or {}
        if not isinstance(bootstrap, dict):
            raise TypeError("Orderbook 'bootstrap' must be a dict")
        self.bootstrap_cfg = dict(bootstrap)

        cache = kwargs.get("cache") or {}
        if not isinstance(cache, dict):
            raise TypeError("Orderbook 'cache' must be a dict")
        self.cache_cfg = dict(cache)

        # cache depth precedence:
        #   1) cache.max_snaps
        #   2) legacy window
        #   3) default
        max_snaps = self.cache_cfg.get("max_snaps")
        if max_snaps is None:
            max_snaps = kwargs.get("window", 200)
        max_snaps_i = int(max_snaps)
        if max_snaps_i <= 0:
            raise ValueError("Orderbook cache.max_snaps must be > 0")

        self.cache = OrderbookCache(maxlen=max_snaps_i)
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
            raise TypeError("Orderbook 'gap' must be a dict")
        min_gap_ms = gap_cfg.get("min_gap_ms")
        self.gap_min_gap_ms = int(min_gap_ms) if min_gap_ms is not None else None
        self._backfill_fn = kwargs.get("backfill_fn") or kwargs.get("backfill")
        self._logger = get_logger(__name__)
        self._anchor_ts = None

        log_debug(
            self._logger,
            "RealTimeOrderbookHandler initialized",
            symbol=self.symbol,
            interval=self.interval,
            max_snaps=max_snaps_i,
            bootstrap=self.bootstrap_cfg,
        )

    # ------------------------------------------------------------------
    # Lifecycle (realtime/mock)
    # ------------------------------------------------------------------

    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        """Preload recent data into cache.

        IO-free by default (no-op). Keeps params for observability/future adapters.
        """
        if lookback is None:
            lookback = self.bootstrap_cfg.get("lookback")

        log_debug(
            self._logger,
            "RealTimeOrderbookHandler.bootstrap (no-op)",
            symbol=self.symbol,
            anchor_ts=anchor_ts,
            lookback=lookback,
        )
        self._maybe_backfill(anchor_ts=anchor_ts, lookback=lookback)

    def align_to(self, ts: int) -> None:
        """Clamp implicit reads to ts (anti-lookahead anchor)."""
        self._anchor_ts = int(ts)
        log_debug(self._logger, "RealTimeOrderbookHandler align_to", symbol=self.symbol, anchor_ts=self._anchor_ts)

    # ------------------------------------------------------------------
    # Streaming tick API
    # ------------------------------------------------------------------

    def on_new_tick(self, payload: Any) -> None:
        """
        Ingest a single orderbook payload (event-time fact).

        Payload contract:
          - Represents an already-occurred orderbook event (event-time).
          - May be:
              * OrderbookSnapshot
              * Mapping[str, Any] with orderbook fields
          - Must contain a resolvable event-time ('data_ts' or 'ts').

        Ingest semantics:
          - Append-only.
          - No visibility decisions (handled by align_to).
        """
        snap = _coerce_snapshot(
            self.symbol,
            payload,
            self.market,
            self.gap_min_gap_ms,
            self.interval_ms,
            last_ts=self.cache.last_timestamp(),
        )
        if snap is None:
            return
        self.cache.push(snap)

    
    # ------------------------------------------------------------------
    # Unified access (timestamp-aligned)
    # ------------------------------------------------------------------

    def last_timestamp(self) -> int | None:
        ts = self.cache.last_timestamp()
        if ts is None:
            return None
        if self._anchor_ts is not None:
            return min(int(ts), int(self._anchor_ts))
        return int(ts)

    def get_snapshot(self, ts: int | None = None) -> OrderbookSnapshot | None:
        """Return the latest OrderbookSnapshot aligned to ts (anti-lookahead)."""
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None

        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        return self.cache.get_at_or_before(int(t))

    def window(self, ts: int | None = None, n: int = 1) -> Iterable[OrderbookSnapshot]:
        """Return last n snapshots aligned to ts (anti-lookahead)."""
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return []

        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        return self.cache.get_n_before(int(t), int(n))

    # ------------------------------------------------------------------
    # Admin / tests
    # ------------------------------------------------------------------

    def reset(self) -> None:
        log_info(self._logger, "RealTimeOrderbookHandler reset requested", symbol=self.symbol)
        self.cache.clear()

    def _maybe_backfill(self, *, anchor_ts: int | None, lookback: Any | None) -> None:
        if self._backfill_fn is None or anchor_ts is None:
            return
        window_ms = _coerce_lookback_ms(lookback, self.interval_ms)
        if window_ms is None:
            return
        start_ts = int(anchor_ts) - int(window_ms)
        if self.cache.get_at_or_before(start_ts) is not None:
            return
        for row in self._backfill_fn(start_ts=int(start_ts), end_ts=int(anchor_ts)):
            self.on_new_tick(row)

    def run_mock(self, df, delay: float = 0.0):
        """v4-compliant simulated orderbook stream."""
        log_info(
            self._logger,
            "RealTimeOrderbookHandler starting mock stream",
            symbol=self.symbol,
            rows=len(df),
            delay=delay,
        )

        for _, row in df.iterrows():
            raw = row.to_dict()

            snapshot = OrderbookSnapshot.from_tick_aligned(
                timestamp=int(raw["data_ts"] if "data_ts" in raw else raw["ts"]),
                tick=raw,
                symbol=self.symbol,
                market=_resolve_market(
                    self.market,
                    raw,
                    last_ts=self.cache.last_timestamp(),
                    data_ts=int(raw.get("data_ts", raw.get("ts"))),
                    expected_interval_ms=self.interval_ms,
                    min_gap_ms=self.gap_min_gap_ms,
                ),
            )

            self.on_new_tick(snapshot)
            window = self.window(snapshot.data_ts)
            yield snapshot, window

            if delay > 0:
                time.sleep(delay)


def _coerce_ts(x: Any) -> int | None:
    if x is None:
        return None
    if isinstance(x, bool):
        return None
    if isinstance(x, int):
        return int(x)
    if isinstance(x, float):
        return int(x)
    try:
        return int(x)  # strings, numpy scalars
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None


def _coerce_snapshot(
    symbol: str,
    x: Any,
    base_market: MarketSpec,
    min_gap_ms: int | None,
    expected_interval_ms: int | None,
    *,
    last_ts: int | None,
) -> OrderbookSnapshot | None:
    if x is None:
        return None
    if isinstance(x, OrderbookSnapshot):
        return x
    if isinstance(x, dict):
        # tolerate alternative keys
        ts = _coerce_ts(x.get("data_ts", x.get("ts")))
        if ts is None:
            return None
        market = _resolve_market(
            base_market,
            x,
            last_ts=last_ts,
            data_ts=int(ts),
            expected_interval_ms=expected_interval_ms,
            min_gap_ms=min_gap_ms,
        )
        return OrderbookSnapshot.from_tick_aligned(
            timestamp=int(ts),
            tick=x,
            symbol=symbol,
            market=market,
        )
    return None


def _resolve_market(
    base: MarketSpec,
    payload: dict[str, Any],
    *,
    last_ts: int | None,
    data_ts: int | None,
    expected_interval_ms: int | None,
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
        expected_interval_ms=expected_interval_ms,
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
        if interval_ms is not None:
            return int(float(lookback) * int(interval_ms))
        return int(float(lookback))
    return None
