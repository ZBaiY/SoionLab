from __future__ import annotations

from typing import Any, Mapping

import pandas as pd
import time

from quant_engine.utils.logger import get_logger, log_debug, log_info
from ingestion.contracts.tick import IngestionTick, _coerce_epoch_ms
from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler
from quant_engine.data.contracts.snapshot import (
    MarketSpec,
    ensure_market_spec,
    merge_market_spec,
    classify_gap,
)
from .cache import (
    OptionChainCache,
    OptionChainSimpleCache,
    OptionChainExpiryCache,
    OptionChainTermBucketedCache,
)
from .snapshot import OptionChainSnapshot


class OptionChainDataHandler(RealTimeDataHandler):
    """Runtime Option-Chain handler (mode-agnostic).

    Domain semantics:
      - Stores immutable OptionChainSnapshot (observation-time facts)
      - Observation/event time is snapshot timestamp (epoch ms) -> snapshot.data_ts
      - No system time / latency stored in snapshots

    Read semantics (anti-lookahead):
      - align_to(ts) sets an anchor; all reads clamp to min(ts, anchor)
      - cache ordering is by data_ts (observation time)

    Polling cadence is external to this handler; it does not own IO.

    Config mapping (Strategy.DATA.*.option_chain):
      - source: origin identifier (e.g., "DERIBIT") (kept for metadata/routing)
      - cache.maxlen: global snapshot cache maxlen (note: each snapshot can be large)
      - cache.per_expiry_maxlen: per-expiry bucket maxlen (stores snapshot refs)
      - columns: dataframe view columns for `chain_df()`
    """

    symbol: str
    source: str
    columns: list[str] | None
    market: MarketSpec
    gap_min_gap_ms: int | None
    bootstrap_cfg: dict[str, Any] | None

    cache_cfg: dict[str, Any]
    cache: OptionChainCache

    _anchor_ts: int | None
    _logger: Any
    _backfill_fn: Any | None

    def __init__(self, symbol: str, **kwargs: Any):
        self.symbol = symbol
        self.source = str(kwargs.get("source") or "DERIBIT")

        cache = kwargs.get("cache") or {}
        if not isinstance(cache, dict):
            raise TypeError("option_chain 'cache' must be a dict")
        self.cache_cfg = dict(cache)
        self.bootstrap_cfg = kwargs.get("bootstrap") or None

        maxlen = int(self.cache_cfg.get("maxlen", kwargs.get("maxlen", 512)))
        if maxlen <= 0:
            raise ValueError("option_chain cache.maxlen must be > 0")

        # cache kind: simple | expiry | term
        kind = str(self.cache_cfg.get("kind") or self.cache_cfg.get("type") or "expiry").lower()

        if kind in {"simple", "deque"}:

            self.cache = OptionChainSimpleCache(maxlen=maxlen)
            per_expiry_maxlen = None
            per_term_maxlen = None
            term_bucket_ms = None
        elif kind in {"term", "term_bucket", "bucketed"}:
            per_term_maxlen = int(self.cache_cfg.get("per_term_maxlen", kwargs.get("per_term_maxlen", 256)))
            term_bucket_ms = int(self.cache_cfg.get("term_bucket_ms", kwargs.get("term_bucket_ms", 86_400_000)))
            if per_term_maxlen <= 0:
                raise ValueError("option_chain cache.per_term_maxlen must be > 0")
            if term_bucket_ms <= 0:
                raise ValueError("option_chain cache.term_bucket_ms must be > 0")

            # optional expiry index inside term cache
            enable_expiry_index = bool(self.cache_cfg.get("enable_expiry_index", True))
            per_expiry_maxlen = self.cache_cfg.get("per_expiry_maxlen", kwargs.get("per_expiry_maxlen"))
            per_expiry_maxlen_i = int(per_expiry_maxlen) if per_expiry_maxlen is not None else None

            self.cache = OptionChainTermBucketedCache(
                maxlen=maxlen,
                per_term_maxlen=per_term_maxlen,
                term_bucket_ms=term_bucket_ms,
                per_expiry_maxlen=per_expiry_maxlen_i,
                enable_expiry_index=enable_expiry_index,
            )
        else:
            # default: expiry-indexed cache
            per_expiry_maxlen = int(self.cache_cfg.get("per_expiry_maxlen", kwargs.get("per_expiry_maxlen", 256)))
            if per_expiry_maxlen <= 0:
                raise ValueError("option_chain cache.per_expiry_maxlen must be > 0")
            per_term_maxlen = None
            term_bucket_ms = None
            self.cache = OptionChainExpiryCache(maxlen=maxlen, per_expiry_maxlen=per_expiry_maxlen)

        # dataframe view columns only (storage keeps full record dicts)
        self.columns = kwargs.get(
            "columns",
            [
                "data_ts",
                "instrument_name",
                "expiry_ts",
                "strike",
                "cp",
                "bid",
                "ask",
                "mark",
                "index_price",
                # fetched IVs are under aux with *_fetch suffix (schema v2)
                "mark_iv_fetch",
                "bid_iv_fetch",
                "ask_iv_fetch",
                "iv_fetch",
                # optional carry-through
                "oi",
                "volume",
            ],
        )

        self.market = ensure_market_spec(
            kwargs.get("market"),
            default_venue=str(kwargs.get("venue", kwargs.get("source", self.source))),
            default_asset_class=str(kwargs.get("asset_class", "option")),
            default_timezone=str(kwargs.get("timezone", "UTC")),
            default_calendar=str(kwargs.get("calendar", "24x7")),
            default_session=str(kwargs.get("session", "24x7")),
            default_currency=kwargs.get("currency"),
        )

        gap_cfg = kwargs.get("gap") or {}
        if not isinstance(gap_cfg, dict):
            raise TypeError("option_chain 'gap' must be a dict")
        min_gap_ms = gap_cfg.get("min_gap_ms")
        self.gap_min_gap_ms = int(min_gap_ms) if min_gap_ms is not None else None

        self._backfill_fn = kwargs.get("backfill_fn") or kwargs.get("backfill")
        self._anchor_ts = None
        self._logger = get_logger(__name__)

        log_debug(
            self._logger,
            "OptionChainDataHandler initialized",
            symbol=self.symbol,
            source=self.source,
            cache_kind=kind,
            maxlen=maxlen,
            per_expiry_maxlen=per_expiry_maxlen,
            per_term_maxlen=per_term_maxlen,
            term_bucket_ms=term_bucket_ms,
        )

    # ------------------------------------------------------------------
    # Lifecycle (realtime/mock)
    # ------------------------------------------------------------------

    def align_to(self, ts: int) -> None:
        """Set observation-time anchor (anti-lookahead)."""
        self._anchor_ts = int(ts)
        log_debug(self._logger, "OptionChainDataHandler align_to", symbol=self.symbol, anchor_ts=self._anchor_ts)

    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        # IO-free by default
        log_debug(
            self._logger,
            "OptionChainDataHandler.bootstrap (no-op)",
            symbol=self.symbol,
            anchor_ts=anchor_ts,
            lookback=lookback,
        )
        self._maybe_backfill(anchor_ts=anchor_ts, lookback=lookback)

    def load_history(
        self,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> None:
        log_debug(
            self._logger,
            "OptionChainDataHandler.load_history (no-op)",
            symbol=self.symbol,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    def warmup_to(self, ts: int) -> None:
        self.align_to(ts)

    # ------------------------------------------------------------------
    # Streaming tick API
    # ------------------------------------------------------------------

    def on_new_tick(self, tick: IngestionTick) -> None:
        """Ingest one snapshot tick.

        Accepted payloads (tick.payload):
          - OptionChainSnapshot
          - Mapping[str, Any] with keys {data_ts, records} (and optional metadata)
          - Mapping[str, Any] representing a *single record* (will be treated as a one-record snapshot)
          - Sequence[Mapping[str, Any]] / pandas.DataFrame as records (requires explicit data_ts)

        Notes:
          - schema_version is defaulted to 2 in snapshot builder.
          - fetched IV fields (iv/mark_iv/bid_iv/ask_iv) are moved into record["aux"] as *_fetch.
        """
        payload = dict(tick.payload)
        if "data_ts" not in payload:
            payload["data_ts"] = int(tick.data_ts)
        snap = _build_snapshot_from_payload(payload, symbol=self.symbol, market=self.market)
        if snap is None:
            return

        # allow passing an already-built snapshot
        if isinstance(payload, OptionChainSnapshot):
            self.cache.push(snap)
            return

        # update market gap classification (best-effort)
        last = self.cache.last()
        last_ts = int(last.data_ts) if last is not None else None
        self._set_gap_market(snap, last_ts=last_ts)

        self.cache.push(snap)
        log_debug(self._logger, "OptionChainDataHandler.on_new_tick", symbol=self.symbol, data_ts=int(snap.data_ts), n_rows=int(len(snap.frame)))

    # ------------------------------------------------------------------
    # Unified access (timestamp-aligned)
    # ------------------------------------------------------------------

    def last_timestamp(self) -> int | None:
        last = self.cache.last()
        if last is None:
            return None
        ts = int(last.data_ts)
        if self._anchor_ts is not None:
            return min(ts, int(self._anchor_ts))
        return ts

    def get_snapshot(self, ts: int | None = None) -> OptionChainSnapshot | None:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        return self.cache.get_at_or_before(t)

    def window(self, ts: int | None = None, n: int = 1) -> list[OptionChainSnapshot]:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return []
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        return list(self.cache.get_n_before(t, int(n)))

    # ------------------------------------------------------------------
    # Expiry helpers (optional)
    # ------------------------------------------------------------------

    def expiries(self) -> list[int]:
        if hasattr(self.cache, "expiries"):
            try:
                return list(self.cache.expiries())  # type: ignore[attr-defined]
            except Exception:
                return []
        return []

    def get_snapshot_for_expiry(self, *, expiry_ts: int, ts: int | None = None) -> OptionChainSnapshot | None:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        if not hasattr(self.cache, "get_at_or_before_for_expiry"):
            return None
        return self.cache.get_at_or_before_for_expiry(int(expiry_ts), t)  # type: ignore[attr-defined]

    # ------------------------------------------------------------------
    # Term helpers (optional; requires term-bucketed cache)
    # ------------------------------------------------------------------

    def term_buckets(self) -> list[int]:
        if hasattr(self.cache, "term_buckets"):
            try:
                return list(self.cache.term_buckets())  # type: ignore[attr-defined]
            except Exception:
                return []
        return []

    def get_snapshot_for_term(self, *, term_key_ms: int, ts: int | None = None) -> OptionChainSnapshot | None:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        if not hasattr(self.cache, "get_at_or_before_for_term"):
            return None
        return self.cache.get_at_or_before_for_term(int(term_key_ms), t)  # type: ignore[attr-defined]

    def window_for_term(self, *, term_key_ms: int, n: int = 1) -> list[OptionChainSnapshot]:
        ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
        if ts is None:
            return []
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        if not hasattr(self.cache, "get_n_before_for_term"):
            return []
        return list(self.cache.get_n_before_for_term(int(term_key_ms), t, int(n)))  # type: ignore[attr-defined]

    # ------------------------------------------------------------------
    # Views
    # ------------------------------------------------------------------

    def chain_df(self, ts: int | None = None, *, columns: list[str] | None = None) -> pd.DataFrame:
        """Return a DataFrame view of the chain at time ts.

        `OptionChainSnapshot` stores a normalized `frame` with an `aux` dict per row.
        This view can surface aux keys as columns on demand.
        """
        snap = self.get_snapshot(ts)
        if snap is None:
            return pd.DataFrame()

        base = snap.frame
        if base is None or len(base) == 0:
            return pd.DataFrame()

        cols = self.columns if columns is None else columns
        if cols is None:
            out = base.copy()
            out.insert(0, "data_ts", int(snap.data_ts))
            return out

        out = pd.DataFrame({"data_ts": [int(snap.data_ts)] * len(base)})
        aux_series = base["aux"] if "aux" in base.columns else None

        for c in cols:
            if c == "data_ts":
                out[c] = int(snap.data_ts)
                continue
            if c in base.columns:
                out[c] = base[c]
                continue
            if aux_series is not None:
                out[c] = aux_series.map(lambda d, key=c: d.get(key) if isinstance(d, dict) else None)
            else:
                out[c] = None

        return out

    # ------------------------------------------------------------------
    # Legacy / misc
    # ------------------------------------------------------------------

    def reset(self) -> None:
        log_info(self._logger, "OptionChainDataHandler reset requested", symbol=self.symbol)
        self.cache.clear()

    def _set_gap_market(self, snap: OptionChainSnapshot, *, last_ts: int | None) -> None:
        # We do not mutate snapshot.market (snap is frozen). Gap classification is best-effort
        # and must live in handler-level market spec.
        #
        # If upstream provides market override/status, merge it into handler market.
        payload: dict[str, Any] = {}
        # currently no standardized market override for option_chain payloads.
        data_ts = int(snap.data_ts)
        gap_type = classify_gap(
            status=None,
            last_ts=last_ts,
            data_ts=data_ts,
            expected_interval_ms=None,
            min_gap_ms=self.gap_min_gap_ms,
        )
        self.market = merge_market_spec(self.market, payload.get("market"), status=None, gap_type=gap_type)

    def _maybe_backfill(self, *, anchor_ts: int | None, lookback: Any | None) -> None:
        if self._backfill_fn is None or anchor_ts is None:
            return
        window_ms = _coerce_lookback_ms(lookback, None)
        if window_ms is None:
            return
        start_ts = int(anchor_ts) - int(window_ms)
        if self.cache.get_at_or_before(start_ts) is not None:
            return
        for snap_payload in self._backfill_fn(start_ts=int(start_ts), end_ts=int(anchor_ts)):
            self.on_new_tick(_tick_from_payload(snap_payload, symbol=self.symbol))


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------


def _now_ms() -> int:
    return int(time.time() * 1000)


def _tick_from_payload(payload: Mapping[str, Any], *, symbol: str) -> IngestionTick:
    data_ts = _infer_data_ts(payload)
    return IngestionTick(
        timestamp=int(data_ts),
        data_ts=int(data_ts),
        domain="option_chain",
        symbol=symbol,
        payload=payload,
    )


def _infer_data_ts(payload: Mapping[str, Any]) -> int:
    ts_any = payload.get("data_ts") or payload.get("timestamp")
    if ts_any is None:
        raise ValueError("Option chain payload missing data_ts/timestamp for backfill")
    return _coerce_epoch_ms(ts_any)


def _build_snapshot_from_payload(payload: Any, *, symbol: str, market: MarketSpec) -> OptionChainSnapshot | None:
    # Case 0: already a snapshot
    if isinstance(payload, OptionChainSnapshot):
        return payload

    # Case 1: direct DataFrame (caller does not provide a timestamp)
    if isinstance(payload, pd.DataFrame):
        try:
            return OptionChainSnapshot.from_chain_aligned(
                data_ts=_now_ms(),
                chain=payload,
                symbol=symbol,
                market=market,
                schema_version=2,
            )
        except Exception:
            return None

    # Case 2: mapping wrapper
    if isinstance(payload, Mapping):
        d = {str(k): v for k, v in payload.items()}

        ts_any = d.get("data_ts") or d.get("timestamp")
        ts = int(ts_any) if ts_any is not None else _now_ms()

        chain = d.get("chain")
        if chain is None:
            chain = d.get("frame")
        if chain is None:
            chain = d.get("records")

        if isinstance(chain, pd.DataFrame):
            try:
                return OptionChainSnapshot.from_chain_aligned(
                    data_ts=ts,
                    chain=chain,
                    symbol=symbol,
                    market=market,
                    schema_version=int(d.get("schema_version") or 2),
                )
            except Exception:
                return None

    return None


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
