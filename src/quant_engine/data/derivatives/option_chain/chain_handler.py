from __future__ import annotations

import math
from typing import Any, Iterable, Mapping, cast
import copy

import pandas as pd
import numpy as np

from quant_engine.utils.logger import get_logger, log_debug, log_info, log_warn, log_exception, log_throttle, throttle_key
from ingestion.contracts.tick import IngestionTick
from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler, to_interval_ms
from quant_engine.data.contracts.snapshot import (
    MarketSpec,
    ensure_market_spec,
    merge_market_spec,
    classify_gap,
)
from quant_engine.runtime.modes import EngineMode
from quant_engine.utils.cleaned_path_resolver import resolve_cleaned_paths, base_asset_from_symbol, resolve_domain_symbol_keys
from quant_engine.utils.paths import resolve_data_root
from ingestion.option_chain.source import OptionChainFileSource
from .cache import (
    OptionChainCache,
    OptionChainSimpleCache,
    OptionChainExpiryCache,
    OptionChainTermBucketedCache,
)
from .snapshot import (
    OptionChainSnapshot,
    OptionChainSnapshotView,
    _CHAIN_COLS,
    _QUOTE_COLS,
    _UNDERLYING_COLS,
    _snapshot_with_annotation,
)
from .helpers import (
    _tick_from_payload,
    _infer_data_ts,
    _build_snapshot_from_payload,
    _coerce_lookback_ms,
    _coerce_lookback_bars,
    _coerce_engine_mode,
    _resolve_source_id,
    _deep_merge,
    _resolve_option_chain_config,
    _validate_option_chain_config,
    _coerce_cp,
    _market_ts_ref,
    _market_ts_ref_info,
    _resolve_underlying,
    _apply_quality_checks,
    _coerce_quality_mode,
    _severity_for,
    _empty_meta,
    _clone_meta,
    _merge_meta,
    _add_reason,
    _finalize_meta,
    _set_selection_context,
    _apply_selection_slice,
    _select_point_from_slice,
    _iter_ts,
    _empty_df_like,
    _to_float_scalar,
    _to_int_scalar,
    _compute_tau_series,
    _compute_x_series,
    _qc_report_from_meta,
    _annotate_snapshot_rows,
    _row_policy_hash,
)


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
      - cache.default_expiry_window: default n for expiry window helpers
      - cache.default_term_window: default n for term window helpers
      - columns: dataframe view columns for `chain_df()`
    """

    symbol: str
    asset: str
    source: str
    interval: str | None
    interval_ms: int | None
    columns: list[str] | None
    market: MarketSpec
    gap_min_gap_ms: int | None
    bootstrap_cfg: dict[str, Any] | None

    cache_cfg: dict[str, Any]
    cache: OptionChainCache
    term_bucket_ms: int
    quality_mode: str
    quality_cfg: dict[str, Any]
    coords_cfg: dict[str, Any]
    selection_cfg: dict[str, Any]
    market_ts_ref_method: str
    config: dict[str, Any]
    reason_severity: dict[str, dict[str, str]]

    _anchor_ts: int | None
    _logger: Any
    _backfill_worker: Any | None
    _backfill_emit: Any | None
    _engine_mode: EngineMode | None
    _data_root: Any
    _market_cache: dict[tuple[Any, ...], pd.DataFrame]
    _coords_aux_cache: dict[tuple[Any, ...], dict[str, Any]] # auxiliary metadata from coordinate mapping (e.g., market_ts_ref)
    _select_tau_cache: dict[tuple[Any, ...], dict[str, Any]] # auxiliary metadata from tau selection (e.g., selected_tau, selection_context)
    _qc_cache: dict[tuple[Any, ...], dict[str, Any]]
    _qc_index: dict[tuple[Any, ...], tuple[Any, ...]]
    _market_keys_by_ts: dict[int, list[tuple[Any, ...]]]     # mapping from snapshot_ts to list of market_cache keys for incremental eviction
    _coords_aux_keys_by_ts: dict[int, list[tuple[Any, ...]]] # mapping from snapshot_ts to list of coords_aux_cache keys for incremental eviction
    _select_tau_keys_by_ts: dict[int, list[tuple[Any, ...]]] # mapping from snapshot_ts to list of select_tau_cache keys for incremental eviction
    _qc_keys_by_ts: dict[int, list[tuple[Any, ...]]]         # mapping from snapshot_ts to list of qc_cache keys for incremental eviction

    def __init__(self, symbol: str, **kwargs: Any):
        self.symbol = symbol
        self.source = str(kwargs.get("source") or "DERIBIT")

        # --- Normalize convenience kwargs into a single option_chain config override:
        preset = kwargs.pop("preset", None)                     # preset: name of GLOBAL_PRESETS entry to use as the base option_chain config
        config = kwargs.pop("config", None)                     # config: explicit option_chain config dict (overrides preset if provided)

        # coords -> tau/x/ATM coordinate mapping; everything is merged into GLOBAL_PRESETS, never hard-coded.
        coord_override = {
            k: kwargs.pop(k)
            for k in ("tau_def", "x_axis", "atm_def", "underlying_field", "price_field", "cp_policy")
            if k in kwargs
        }                                                        
        # tau_def: tau anchor (market_ts/data_ts); x_axis: moneyness/delta; atm_def: ATM rule; cp_policy: call/put selection

        if "coords" in kwargs:
            raw_coords = kwargs.pop("coords")                    # coords: full coordinate mapping override dict
            if isinstance(raw_coords, dict):
                coord_override = _deep_merge(raw_coords, coord_override) if coord_override else dict(raw_coords)
            else:
                raise TypeError("option_chain coords must be a dict")

        selection_override = {}                                  # selection: how expiries/x-points are chosen/interpolated
        if "selection_method" in kwargs:
            selection_override["method"] = kwargs.pop("selection_method")   # method: nearest/bracket/weighted expiry selection
        if "selection_interp" in kwargs:
            selection_override["interp"] = kwargs.pop("selection_interp")   # interp: interpolation scheme within expiry/x grid

        quality_override = kwargs.pop("quality", None) if "quality" in kwargs else None          # quality: QC thresholds (coverage, spread, OI, staleness, etc.)
        quality_mode_override = kwargs.pop("quality_mode", None) if "quality_mode" in kwargs else None  # quality_mode: STRICT/TRADING/RESEARCH semantics

        cache_override = kwargs.pop("cache", None) if "cache" in kwargs else None                 # cache: snapshot cache config (kind/maxlen/etc.)
        market_ts_ref_method_override = kwargs.pop("market_ts_ref_method", None) if "market_ts_ref_method" in kwargs else None  # market_ts_ref_method: how snapshot market_ts is derived from quotes
        term_bucket_override = kwargs.pop("term_bucket_ms", None) if "term_bucket_ms" in kwargs else None  # term_bucket_ms: coarse term prefilter granularity (not economic tau)

        config_override: dict[str, Any] = {}                   # config_override: normalized option_chain config passed to DataHandler
        if cache_override is not None:
            config_override["cache"] = cache_override
        if quality_override is not None:
            config_override["quality"] = quality_override
        if quality_mode_override is not None:
            config_override["quality_mode"] = quality_mode_override
        if coord_override:
            config_override["coords"] = coord_override
        if "selection" in kwargs:
            raw_sel = kwargs.pop("selection")                   # selection: full selection override dict (merged with method/interp)
            if isinstance(raw_sel, dict):
                selection_override = _deep_merge(raw_sel, selection_override) if selection_override else dict(raw_sel)
            else:
                raise TypeError("option_chain selection must be a dict")
        if selection_override:
            config_override["selection"] = selection_override
        if market_ts_ref_method_override is not None:
            config_override["market_ts_ref_method"] = market_ts_ref_method_override
        if term_bucket_override is not None:
            config_override["term_bucket_ms"] = term_bucket_override

        interval = kwargs.get("interval")
        if interval is not None and (not isinstance(interval, str) or not interval):
            raise ValueError("option_chain 'interval' must be a non-empty string if provided")
        self.interval = interval
        interval_ms = to_interval_ms(self.interval) if self.interval is not None else None
        self.interval_ms = int(interval_ms) if interval_ms is not None else None

        resolved_cfg = _resolve_option_chain_config(              # resolve final option_chain config from preset + base config + overrides
            preset=preset,
            config=config,
            override=config_override,
            interval_ms=self.interval_ms,
        )
        self.config = resolved_cfg                               # store resolved, immutable option_chain config

        asset_any = kwargs.get("asset") or kwargs.get("currency") or kwargs.get("underlying")  # asset hint for symbol normalization
        self.asset = base_asset_from_symbol(str(asset_any)) if asset_any is not None else base_asset_from_symbol(symbol)

        self._engine_mode = _coerce_engine_mode(kwargs.get("mode"))  # engine mode (backtest/realtime/mock) affects filtering & IO
        self._data_root = resolve_data_root(                     # data root for persistence and backfill
            __file__,
            levels_up=5,
            data_root=kwargs.get("data_root") or kwargs.get("cleaned_root"),
        )
        self.source_id = _resolve_source_id(                     # source_id used for tick filtering / provenance
            source_id=kwargs.get("source_id"),
            mode=self._engine_mode,
            data_root=self._data_root,
            source=self.source,
        )

        cache = resolved_cfg.get("cache") or {}                  # cache config block (kind/maxlen/windows/etc.)
        if not isinstance(cache, dict):
            raise TypeError("option_chain 'cache' must be a dict")
        self.cache_cfg = dict(cache)
        self.bootstrap_cfg = kwargs.get("bootstrap") or None     # optional bootstrap lookback config

        maxlen_any = self.cache_cfg.get("maxlen")                # global snapshot cache length
        if maxlen_any is None:
            raise ValueError("option_chain cache.maxlen must be provided")
        maxlen = int(maxlen_any)
        if maxlen <= 0:
            raise ValueError("option_chain cache.maxlen must be > 0")

        kind = str(self.cache_cfg.get("kind") or self.cache_cfg.get("type") or "expiry").lower()  # cache strategy: simple|expiry|term

        default_expiry_any = self.cache_cfg.get("default_expiry_window")  # default expiry slice window
        default_term_any = self.cache_cfg.get("default_term_window")      # default term slice window
        if default_expiry_any is None or default_term_any is None:
            raise ValueError("option_chain cache default windows must be provided")
        default_expiry_window = int(default_expiry_any)
        default_term_window = int(default_term_any)
        if default_expiry_window <= 0:
            raise ValueError("option_chain cache.default_expiry_window must be > 0")
        if default_term_window <= 0:
            raise ValueError("option_chain cache.default_term_window must be > 0")

        if kind in {"simple", "deque"}:
            self.cache = OptionChainSimpleCache(maxlen=maxlen)   # pure time-ordered snapshot cache
            term_bucket_ms = None
        elif kind in {"term", "term_bucket", "bucketed"}:
            term_bucket_any = self.cache_cfg.get("term_bucket_ms")  # coarse term prefilter granularity
            if term_bucket_any is None:
                raise ValueError("option_chain cache.term_bucket_ms must be provided")
            term_bucket_ms = int(term_bucket_any)
            if term_bucket_ms <= 0:
                raise ValueError("option_chain cache.term_bucket_ms must be > 0")
            self.cache = OptionChainTermBucketedCache(            # cache with term-bucket helpers
                maxlen=maxlen,
                term_bucket_ms=term_bucket_ms,
                default_term_window=default_term_window,
                default_expiry_window=default_expiry_window,
            )
        else:
            term_bucket_ms = None
            self.cache = OptionChainExpiryCache(                  # cache with expiry-index helpers
                maxlen=maxlen,
                default_expiry_window=default_expiry_window,
            )

        cols_any = kwargs.get("columns")                          # optional column projection for snapshots
        self.columns = list(cols_any) if cols_any is not None else None

        if term_bucket_ms is None:
            term_bucket_any = self.cache_cfg.get("term_bucket_ms")  # ensure term_bucket_ms is always defined
            if term_bucket_any is None:
                raise ValueError("option_chain cache.term_bucket_ms must be provided")
            term_bucket_ms = int(term_bucket_any)
        self.term_bucket_ms = int(term_bucket_ms)
        if self.term_bucket_ms <= 0:
            raise ValueError("option_chain term_bucket_ms must be > 0")

        self.quality_mode = _coerce_quality_mode(resolved_cfg.get("quality_mode"))  # QC strictness (STRICT/TRADING/RESEARCH)
        quality_cfg = resolved_cfg.get("quality") or {}           # QC thresholds and limits
        if not isinstance(quality_cfg, dict):
            raise TypeError("option_chain 'quality' must be a dict")
        self.quality_cfg = dict(quality_cfg)
        self.reason_severity = dict(self.quality_cfg.get("reason_severity") or {})  # reason_code -> severity mapping
        self.row_policy_cfg = dict(self.quality_cfg.get("row_policy") or {})  # row-level eligibility policy (already normalized by validator)
        self.row_policy_hash = _row_policy_hash(self.row_policy_cfg)          # deterministic config hash for cache key
        self.liquidity_gate_cfg = dict(self.quality_cfg.get("liquidity_gate") or {})  # selection-level liquidity gate config

        coords_cfg = resolved_cfg.get("coords") or {}             # coordinate mapping config (tau/x/ATM definitions)
        if not isinstance(coords_cfg, dict):
            raise TypeError("option_chain 'coords' must be a dict")
        self.coords_cfg = dict(coords_cfg)

        selection_cfg = resolved_cfg.get("selection") or {}       # expiry/x selection + interpolation rules
        if not isinstance(selection_cfg, dict):
            raise TypeError("option_chain 'selection' must be a dict")
        self.selection_cfg = dict(selection_cfg)

        self.market_ts_ref_method = str(resolved_cfg.get("market_ts_ref_method"))  # how snapshot market_ts is derived from quote data

        self.market = ensure_market_spec(                         # canonical market metadata for snapshots
            kwargs.get("market"),
            default_venue=str(kwargs.get("venue", kwargs.get("source", self.source))),
            default_asset_class=str(kwargs.get("asset_class", "option")),
            default_timezone=str(kwargs.get("timezone", "UTC")),
            default_calendar=str(kwargs.get("calendar", "24x7")),
            default_session=str(kwargs.get("session", "24x7")),
            default_currency=kwargs.get("currency"),
        )
        self.display_symbol, self._symbol_aliases = resolve_domain_symbol_keys(  # normalized symbol + aliases for routing
            "option_chain",
            self.symbol,
            self.asset,
            getattr(self.market, "currency", None),
        )

        gap_cfg = kwargs.get("gap") or {}
        if not isinstance(gap_cfg, dict):
            raise TypeError("option_chain 'gap' must be a dict")
        min_gap_ms = gap_cfg.get("min_gap_ms")
        self.gap_min_gap_ms = int(min_gap_ms) if min_gap_ms is not None else None

        # Backfill worker is wired by runtime/apps; do not initialize here.
        self._backfill_worker = None
        self._backfill_emit = None
        self._anchor_ts = None
        self._logger = get_logger(__name__)
        self._market_cache = {}    # cache for market-only frames keyed by snapshot_ts (+ include_underlying flag)
        self._coords_aux_cache = {} # auxiliary metadata from coordinate mapping (e.g., expiry_tau medians)
        self._select_tau_cache = {} # auxiliary metadata from tau selection (e.g., selected_tau, selection_context)
        self._qc_cache = {}         # cached QC reports keyed by snapshot_ts + quality policy + coord defs
        self._qc_index = {}         # (snapshot_ts + policy + coord defs) -> qc_cache key for O(1) lookups
        
        # Handler-side caches are bounded by main cache and evicted incrementally.
        self._market_keys_by_ts = {} # mapping from snapshot_ts to list of market_cache keys for incremental eviction
        self._coords_aux_keys_by_ts = {} # mapping from snapshot_ts to list of coords_aux_cache keys for incremental eviction
        self._select_tau_keys_by_ts = {} # mapping from snapshot_ts to list of select_tau_cache keys for incremental eviction
        self._qc_keys_by_ts = {} # mapping from snapshot_ts to list of qc_cache keys for incremental eviction

        log_debug(
            self._logger,
            "OptionChainDataHandler initialized",
            symbol=self.display_symbol,
            instrument_symbol=self.symbol,
            source=self.source,
            cache_kind=kind,
            maxlen=maxlen,
            default_expiry_window=default_expiry_window,
            default_term_window=default_term_window,
            term_bucket_ms=term_bucket_ms,
        )
        log_debug(
            self._logger,
            "option_chain.config_resolved",
            symbol=self.display_symbol,
            instrument_symbol=self.symbol,
            config=self.config,
        )

    def set_external_source(self, worker: Any | None, *, emit: Any | None = None) -> None:
        """Attach ingestion worker for backfill (wired by runtime/apps)."""
        self._backfill_worker = worker
        self._backfill_emit = emit

    # ------------------------------------------------------------------
    # Lifecycle (realtime/mock)
    # ------------------------------------------------------------------

    def align_to(self, ts: int) -> None:
        """Set observation-time anchor (anti-lookahead)."""
        self._anchor_ts = int(ts)
        log_debug(
            self._logger,
            "OptionChainDataHandler align_to",
            symbol=self.display_symbol,
            instrument_symbol=self.symbol,
            anchor_ts=self._anchor_ts,
        )

    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        # IO-free by default. Bootstrap only reads local storage.
        log_debug(
            self._logger,
            "OptionChainDataHandler.bootstrap (no-op)",
            symbol=self.display_symbol,
            instrument_symbol=self.symbol,
            anchor_ts=anchor_ts,
            lookback=lookback,
        )
        if anchor_ts is None:
            return
        self._bootstrap_from_files(anchor_ts=int(anchor_ts), lookback=lookback)

    def load_history(
        self,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> None:
        log_debug(
            self._logger,
            "OptionChainDataHandler.load_history (no-op)",
            symbol=self.display_symbol,
            instrument_symbol=self.symbol,
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
        if tick.domain != "option_chain" or tick.symbol not in self._symbol_aliases:
            return
        expected_source = getattr(self, "source_id", None)
        tick_source = getattr(tick, "source_id", None)
        if expected_source is not None and tick_source != expected_source:
            return
        payload = dict(tick.payload)
        if "data_ts" not in payload:
            log_warn(
                self._logger,
                "option_chain.missing_data_ts",
                symbol=self.display_symbol,
                instrument_symbol=self.symbol,
                source_id=getattr(tick, "source_id", None),
            )
            return
        snap = _build_snapshot_from_payload(payload, symbol=self.display_symbol, market=self.market)
        if snap is None:
            return
        if snap.chain_frame is None or snap.chain_frame.empty:
            if self.quality_mode != "STRICT":
                log_debug(
                    self._logger,
                    "option_chain.empty_snapshot_skip",
                    symbol=self.display_symbol,
                    instrument_symbol=self.symbol,
                    data_ts=int(snap.data_ts),
                )
                return
        last = self.cache.last()
        if last is not None and int(snap.data_ts) < int(last.data_ts):
            log_warn(
                self._logger,
                "option_chain.out_of_order_arrival",
                symbol=self.display_symbol,
                last_data_ts=int(last.data_ts),
                snap_data_ts=int(snap.data_ts),
                source_id=getattr(tick, "source_id", None),
            )
            return

        # --- Row-level eligibility annotation (before cache push) ---
        # Sub-frames are positionally aligned to chain_frame by _split_frames,
        # so we can extract columns directly without an instrument_name merge.
        if snap.chain_frame is not None and not snap.chain_frame.empty:
            n_rows = len(snap.chain_frame)
            ann_df = pd.DataFrame(index=range(n_rows))
            qf = snap.quote_frame
            for c in ("bid_price", "ask_price", "open_interest", "mark_price"):
                if c in qf.columns and len(qf) == n_rows:
                    ann_df[c] = qf[c].values
            uf = snap.underlying_frame
            if uf is not None and "underlying_price" in uf.columns and len(uf) == n_rows:
                ann_df["underlying_price"] = uf["underlying_price"].values
            _rf, _rm = _annotate_snapshot_rows(ann_df, row_policy_cfg=self.row_policy_cfg)
            snap = _snapshot_with_annotation(snap, row_flags=_rf, row_mask=_rm, row_policy_hash=self.row_policy_hash)

        # update market gap classification (best-effort)
        last_ts = int(last.data_ts) if last is not None else None
        self._set_gap_market(snap, last_ts=last_ts)

        evicted = self.cache.push(snap) ## get the ts for the evited snapshot
        self._evict_from_push(evicted)  ## to prevent overflow of caches
        log_debug(
            self._logger,
            "OptionChainDataHandler.on_new_tick",
            symbol=self.display_symbol,
            instrument_symbol=self.symbol,
            data_ts=int(snap.data_ts),
            n_rows=int(len(snap.chain_frame)),
        )

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

    # ------------------------------------------------------------------
    # Views
    # ------------------------------------------------------------------

    def chain_df(
        self,
        ts: int | None = None,
        *,
        columns: list[str] | None = None,
        include_underlying: bool = False,
        include_aux: bool = False,
    ) -> pd.DataFrame:
        """Return a DataFrame view of the chain at time ts.

        Default view is chain+quote. Underlying columns are included only when
        requested via `include_underlying=True` or explicit `columns`.
        """
        snap = self.get_snapshot(ts)
        if snap is None:
            return pd.DataFrame()

        base = snap.frame
        if base is None or base.empty:
            return pd.DataFrame()

        merged = base

        cols = self.columns if columns is None else columns
        col_set = set(cols) if cols is not None else set()

        need_underlying = bool(include_underlying)
        need_aux = bool(include_aux)
        if cols is not None:
            if snap.underlying_frame is not None and any(c in snap.underlying_frame.columns for c in col_set):
                need_underlying = True
            if snap.aux_frame is not None and any(c in snap.aux_frame.columns for c in col_set):
                need_aux = True

        if need_underlying and snap.underlying_frame is not None and not snap.underlying_frame.empty:
            extra_cols = [
                c for c in snap.underlying_frame.columns
                if c != "instrument_name" and c not in merged.columns
            ]
            if extra_cols:
                merged = merged.merge(
                    snap.underlying_frame[["instrument_name"] + extra_cols],
                    on="instrument_name",
                    how="left",
                )
        elif (not need_underlying) and snap.underlying_frame is not None and not snap.underlying_frame.empty:
            drop_cols = [
                c for c in snap.underlying_frame.columns
                if c != "instrument_name" and c in merged.columns
            ]
            if drop_cols:
                merged = merged.drop(columns=drop_cols)

        if need_aux and snap.aux_frame is not None and not snap.aux_frame.empty:
            extra_cols = [
                c for c in snap.aux_frame.columns
                if c != "instrument_name" and c not in merged.columns
            ]
            if extra_cols:
                merged = merged.merge(
                    snap.aux_frame[["instrument_name"] + extra_cols],
                    on="instrument_name",
                    how="left",
                )

        if cols is None:
            out = merged.copy()
            if "data_ts" not in out.columns:
                out.insert(0, "data_ts", int(snap.data_ts))
            else:
                out["data_ts"] = int(snap.data_ts)
            return out

        out = pd.DataFrame({"data_ts": [int(snap.data_ts)] * len(merged)})
        for c in cols:
            if c == "data_ts":
                out[c] = int(snap.data_ts)
            elif c in merged.columns:
                out[c] = merged[c]
            else:
                out[c] = None
        return out

    def _market_frame_uncached(self, snap: OptionChainSnapshot, *, include_underlying: bool) -> pd.DataFrame:
        base = snap.frame
        if base is None or base.empty:
            return pd.DataFrame()
        df = base
        if include_underlying and snap.underlying_frame is not None and not snap.underlying_frame.empty:
            extra_cols = [
                c for c in snap.underlying_frame.columns
                if c != "instrument_name" and c not in df.columns and c in _UNDERLYING_COLS
            ]
            if extra_cols:
                df = df.merge(
                    snap.underlying_frame[["instrument_name"] + extra_cols],
                    on="instrument_name",
                    how="left",
                )
        elif (not include_underlying) and snap.underlying_frame is not None and not snap.underlying_frame.empty:
            drop_cols = [
                c for c in snap.underlying_frame.columns
                if c != "instrument_name" and c in df.columns and c in _UNDERLYING_COLS
            ]
            if drop_cols:
                df = df.drop(columns=drop_cols)
        allowed = _CHAIN_COLS | _QUOTE_COLS | _UNDERLYING_COLS
        keep = [c for c in df.columns if c in allowed]
        return df[keep].copy()

    def market_frame(
        self,
        ts: int | None = None,
        *,
        columns: list[str] | None = None,
        include_underlying: bool | None = None,
    ) -> pd.DataFrame:
        """Return market-only columns for a snapshot.

        Invariants: exchange-provided fields only (chain/quote/underlying); MUST NOT include derived or provenance columns.
        This does NOT add data_ts, tau/x, slice tags, or any selection/QC fields; it is safe to cache.
        Underlying columns are included by default (`include_underlying=None` -> True).
        """
        # Audit note: snapshot/slice/selection fields were removed from frames; provenance stays in meta/view attrs.
        snap = self.get_snapshot(ts)
        if snap is None:
            return pd.DataFrame()
        use_underlying = True if include_underlying is None else bool(include_underlying)
        key = (int(snap.data_ts), use_underlying)
        cached = self._market_cache.get(key)
        if cached is None:
            # Market-only frames are immutable per snapshot_ts, so caching is safe.
            cached = self._market_frame_uncached(snap, include_underlying=use_underlying)
            self._market_cache[key] = cached
            self._market_keys_by_ts.setdefault(int(snap.data_ts), []).append(key)
        df = cached
        if columns is None:
            return df.copy()
        allowed = _CHAIN_COLS | _QUOTE_COLS | _UNDERLYING_COLS
        keep = [c for c in columns if c in allowed and c in df.columns]
        if not keep:
            return pd.DataFrame()
        return df[keep].copy()

    # ------------------------------------------------------------------
    # Legacy / misc
    # ------------------------------------------------------------------

    def reset(self) -> None:
        log_info(
            self._logger,
            "OptionChainDataHandler reset requested",
            symbol=self.display_symbol,
            instrument_symbol=self.symbol,
        )
        self.cache.clear()
        self._market_cache.clear()
        self._coords_aux_cache.clear()
        self._select_tau_cache.clear()
        self._qc_cache.clear()
        self._qc_index.clear()
        self._market_keys_by_ts.clear()
        self._coords_aux_keys_by_ts.clear()
        self._select_tau_keys_by_ts.clear()
        self._qc_keys_by_ts.clear()

    def _coords_frame_uncached(
        self,
        snap: OptionChainSnapshot,
        *,
        tau_def: str,
        x_axis: str,
        atm_def: str,
        price_field: str,
        quality_mode: str,
        drop_aux: bool,
    ) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
        """
        Return coordinate-mapped DataFrame view of snapshot.
        Standardize and enrich a snapshot view for selection and QC while keeping provenance in meta.
        The returned DataFrame includes derived tau/x coordinates but no provenance columns.
        """
        data_ts = int(snap.data_ts)
        # drop_aux is kept for API compatibility; market-only frames never include aux_frame columns.
        base = self.market_frame(ts=data_ts, include_underlying=True)
        if base is None or base.empty:
            meta = _empty_meta(snapshot_data_ts=data_ts, snapshot_market_ts=None, quality_mode=quality_mode)
            meta["tau_anchor_ts"] = int(data_ts)
            meta["tau_def"] = str(tau_def)
            meta["market_ts_ref_method"] = None
            meta["x_axis"] = str(x_axis)
            meta["atm_ref"] = None
            meta["underlying_ref"] = None
            _set_selection_context(
                meta,
                tau_def=tau_def,
                x_axis=x_axis,
                atm_def=atm_def,
                price_field=price_field,
            )
            _add_reason(meta, "EMPTY_CHAIN", _severity_for("EMPTY_CHAIN", quality_mode, self.reason_severity), {})
            _finalize_meta(meta, quality_mode)
            self._cache_qc_report(
                meta,
                tau_def=str(tau_def),
                x_axis=str(x_axis),
                atm_def=str(atm_def),
                price_field=str(price_field),
                qc_scope="full_rows",
                row_policy_hash=None,
            )
            return pd.DataFrame(), meta, {}

        df = base.copy()

        if "expiry_ts" not in df.columns and "expiration_timestamp" in df.columns:
            df["expiry_ts"] = pd.to_numeric(df["expiration_timestamp"], errors="coerce")
        if "expiry_ts" not in df.columns:
            df["expiry_ts"] = pd.NA
        df["expiry_ts"] = pd.to_numeric(df["expiry_ts"], errors="coerce").round().astype("Int64")

        if "strike" not in df.columns:
            df["strike"] = pd.NA
        df["strike"] = pd.to_numeric(df["strike"], errors="coerce")

        if "cp" not in df.columns:
            if "option_type" in df.columns:
                df["cp"] = df["option_type"].map(_coerce_cp)
            else:
                df["cp"] = pd.NA

        market_ts_ref, market_ts_ref_method = _market_ts_ref_info(
            df,
            snap,
            method=str(self.market_ts_ref_method),
        )
        snapshot_market_ts = int(market_ts_ref) if market_ts_ref is not None else None

        underlying_field = self.coords_cfg.get("underlying_field") or atm_def
        underlying_ref = _resolve_underlying(df, str(underlying_field)) ## giving the underlying price
        atm_ref = _resolve_underlying(df, str(atm_def)) ## giving the selected atm price

        if tau_def == "data_ts" or snapshot_market_ts is None:
            # Fall back to data_ts when market_ts is unavailable to keep tau anchored to observation time.
            tau_anchor = data_ts
        else:
            tau_anchor = snapshot_market_ts

        # coords_df carries row-level selection fields only; meta holds constants/provenance.
        meta = _empty_meta(snapshot_data_ts=data_ts, snapshot_market_ts=snapshot_market_ts, quality_mode=quality_mode)
        meta["tau_anchor_ts"] = int(tau_anchor)
        meta["tau_def"] = str(tau_def)
        meta["market_ts_ref_method"] = market_ts_ref_method
        meta["x_axis"] = str(x_axis)
        meta["atm_ref"] = atm_ref
        meta["underlying_ref"] = underlying_ref
        if tau_def == "market_ts" and snapshot_market_ts is None:
            _add_reason(meta, "MISSING_MARKET_TS", _severity_for("MISSING_MARKET_TS", quality_mode, self.reason_severity), {})
        _set_selection_context(
            meta,
            tau_def=tau_def,
            x_axis=x_axis,
            atm_def=atm_def,
            price_field=price_field,
        )

        tau_series = _compute_tau_series(df, tau_anchor_ts=int(tau_anchor))
        x_series = _compute_x_series(df, atm_ref=atm_ref, x_axis=str(x_axis))
        df["tau_ms"] = pd.to_numeric(tau_series, errors="coerce").round().astype("Int64")
        df["x"] = x_series

        # --- Row mask for QC scope ---
        snap_mask = getattr(snap, "row_mask", None)
        qc_mask = None
        qc_scope = "full_rows"
        if (
            isinstance(snap_mask, np.ndarray)
            and snap_mask.ndim == 1
            and snap_mask.dtype == np.dtype("bool")
            and len(snap_mask) == len(df)
            and isinstance(df.index, pd.RangeIndex)
            and int(df.index.start) == 0
            and int(df.index.step) == 1
        ):
            aligned = True
            if "instrument_name" in df.columns and "instrument_name" in snap.chain_frame.columns:
                aligned = snap.chain_frame["instrument_name"].reset_index(drop=True).equals(
                    df["instrument_name"].reset_index(drop=True)
                )
            if aligned:
                qc_mask = snap_mask
                qc_scope = "masked_rows"
        meta["qc_scope"] = qc_scope
        meta["row_policy_id"] = self.row_policy_cfg.get("id")
        meta["row_policy_version"] = self.row_policy_cfg.get("version")
        meta["row_policy_hash"] = getattr(snap, "row_policy_hash", None)

        # QC after coordinate derivation yields auditability aligned with the selection view.
        _apply_quality_checks(self, df, meta, quality_mode, self.reason_severity, qc_mask=qc_mask, qc_scope=qc_scope)
        _finalize_meta(meta, quality_mode)
        self._cache_qc_report(
            meta,
            tau_def=str(tau_def),
            x_axis=str(x_axis),
            atm_def=str(atm_def),
            price_field=str(price_field),
            qc_scope=qc_scope,
            row_policy_hash=meta.get("row_policy_hash"),
        )

        # aux includes coordinate-derived metadata that may be useful for selection but is not needed for QC.
        aux = {"expiry_tau": pd.to_numeric(tau_series.groupby(df["expiry_ts"]).median(), errors="coerce").round().astype("Int64")}
        ## groupby is for grouping same expiry options together and taking the median tau as a representative value for the expiry
        ## 相同的expiry的option可能在不同的行上有不同的tau值，取中位数可以得到一个代表性的tau值，反映该expiry的整体时间特征
        # this aux has nothing to do with the aux_frame in the snapshot
        return df, meta, aux

    def _min_n_per_slice(self) -> int:
        ## minimum number of records per slice (expiry or term bucket) for it to be considered valid for selection; 
        # helps prevent overfitting to sparse slices and also speeds up selection by skipping very sparse slices entirely.
        return int(self.quality_cfg["min_n_per_slice"])

    def _liquidity_gate_signature(self, *, row_policy_hash: str | None) -> tuple[Any, ...]:
        cfg = dict(self.liquidity_gate_cfg or {})
        limits = cfg.get("limits") or {}
        return (
            bool(cfg.get("enabled", False)),
            float(cfg.get("x_max", 0.10)),
            int(cfg.get("min_n", 6)),
            float((limits or {}).get("p75_max", 0.20)),
            float((limits or {}).get("p90_max", 0.35)),
            str(row_policy_hash) if row_policy_hash is not None else None,
        )

    def _apply_selection_liquidity_gate(
        self,
        slice_df: pd.DataFrame,
        meta: dict[str, Any],
        *,
        quality_mode: str,
        ts: int | None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        cfg = dict(self.liquidity_gate_cfg or {})
        enabled = bool(cfg.get("enabled", False))
        x_max = float(cfg.get("x_max", 0.10))
        min_n = int(cfg.get("min_n", 6))
        quantiles = cfg.get("quantiles") or {}
        limits = cfg.get("limits") or {}
        q75 = float(quantiles.get("p75", 0.75))
        q90 = float(quantiles.get("p90", 0.90))
        p75_max = float(limits.get("p75_max", 0.20))
        p90_max = float(limits.get("p90_max", 0.35))
        eps = float(self.quality_cfg.get("eps", 1e-12))

        if not enabled:
            meta["liquidity"] = {
                "enabled": False,
                "x_max": x_max,
                "min_n": min_n,
                "n_corridor": 0,
                "corridor_coverage": 0.0,
                "p75": None,
                "p90": None,
                "p75_max": p75_max,
                "p90_max": p90_max,
                "ok": True,
                "state": "OK",
            }
            _finalize_meta(meta, quality_mode)
            return slice_df, meta

        n_rows = int(len(slice_df))
        eligible = np.ones(n_rows, dtype=bool)
        if n_rows > 0:
            snap_ref_ts = meta.get("snapshot_data_ts")
            snap = self.get_snapshot(int(snap_ref_ts)) if snap_ref_ts is not None else self.get_snapshot(ts)
            snap_mask = getattr(snap, "row_mask", None) if snap is not None else None
            if (
                isinstance(snap_mask, np.ndarray)
                and snap_mask.ndim == 1
                and snap_mask.dtype == np.dtype("bool")
                and np.issubdtype(slice_df.index.to_numpy().dtype, np.integer)
            ):
                idx = slice_df.index.to_numpy(dtype=np.int64, copy=False)
                if idx.size > 0 and int(idx.min()) >= 0 and int(idx.max()) < int(len(snap_mask)):
                    aligned = True
                    if snap is not None and "instrument_name" in slice_df.columns and "instrument_name" in snap.chain_frame.columns:
                        snap_names = snap.chain_frame["instrument_name"].iloc[idx].to_numpy(dtype=object, copy=False)
                        slice_names = slice_df["instrument_name"].to_numpy(dtype=object, copy=False)
                        aligned = np.array_equal(snap_names, slice_names)
                    if aligned:
                        eligible = snap_mask[idx]

        x_vals = (
            pd.to_numeric(slice_df["x"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
            if "x" in slice_df.columns
            else np.full(n_rows, np.nan, dtype=float)
        )
        corridor_mask = eligible & np.isfinite(x_vals) & (np.abs(x_vals) <= float(x_max))
        n_corridor = int(corridor_mask.sum())
        corridor_coverage = float(n_corridor) / float(max(1, n_rows))

        spread = np.array([], dtype=float)
        if n_rows > 0 and "bid_price" in slice_df.columns and "ask_price" in slice_df.columns:
            bid = pd.to_numeric(slice_df["bid_price"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
            ask = pd.to_numeric(slice_df["ask_price"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
            if "mid_price" in slice_df.columns:
                mid_like = pd.to_numeric(slice_df["mid_price"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
            elif "mark_price" in slice_df.columns:
                mid_like = pd.to_numeric(slice_df["mark_price"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
            else:
                mid_like = (bid + ask) / 2.0
            spread_mask = corridor_mask & np.isfinite(bid) & np.isfinite(ask) & np.isfinite(mid_like)
            if bool(np.any(spread_mask)):
                denom = np.maximum(np.abs(mid_like[spread_mask]), float(eps))
                spread = ((ask[spread_mask] - bid[spread_mask]) / denom).astype(float, copy=False)
                spread = spread[np.isfinite(spread)]

        p75 = float(np.quantile(spread, q75)) if spread.size > 0 else None
        p90 = float(np.quantile(spread, q90)) if spread.size > 0 else None
        p50 = float(np.quantile(spread, 0.50)) if spread.size > 0 else None
        p95 = float(np.quantile(spread, 0.95)) if spread.size > 0 else None

        liquidity_reasons: list[dict[str, Any]] = []
        if n_corridor < int(min_n):
            _add_reason(
                meta,
                "LIQUIDITY_COVERAGE_LOW",
                _severity_for("LIQUIDITY_COVERAGE_LOW", quality_mode, self.reason_severity),
                {"min_n": int(min_n), "n_corridor": int(n_corridor), "x_max": float(x_max)},
            )
            liquidity_reasons.append({"reason_code": "LIQUIDITY_COVERAGE_LOW"})
        elif p75 is None or p90 is None:
            _add_reason(
                meta,
                "LIQUIDITY_COVERAGE_LOW",
                _severity_for("LIQUIDITY_COVERAGE_LOW", quality_mode, self.reason_severity),
                {"min_n": int(min_n), "n_corridor": int(n_corridor), "n_spread_rows": int(spread.size), "x_max": float(x_max)},
            )
            liquidity_reasons.append({"reason_code": "LIQUIDITY_COVERAGE_LOW"})
        elif p75 > float(p75_max) or p90 > float(p90_max):
            _add_reason(
                meta,
                "LIQUIDITY_SPREAD_WIDE",
                _severity_for("LIQUIDITY_SPREAD_WIDE", quality_mode, self.reason_severity),
                {
                    "p75": float(p75),
                    "p90": float(p90),
                    "p75_max": float(p75_max),
                    "p90_max": float(p90_max),
                    "x_max": float(x_max),
                },
            )
            liquidity_reasons.append({"reason_code": "LIQUIDITY_SPREAD_WIDE"})

        liquidity_state = "OK"
        if any(r.get("reason_code") in {"LIQUIDITY_COVERAGE_LOW", "LIQUIDITY_SPREAD_WIDE"} for r in liquidity_reasons):
            # Severity decides hard/soft state while preserving existing meta style.
            l_reasons = [r for r in (meta.get("reasons") or []) if r.get("reason_code") in {"LIQUIDITY_COVERAGE_LOW", "LIQUIDITY_SPREAD_WIDE"}]
            if any(r.get("severity") == "HARD" for r in l_reasons):
                liquidity_state = "HARD_FAIL"
            else:
                liquidity_state = "SOFT_DEGRADED"
        meta["liquidity"] = {
            "enabled": True,
            "x_axis": "log_moneyness",
            "x_max": float(x_max),
            "min_n": int(min_n),
            "n_corridor": int(n_corridor),
            "corridor_coverage": float(corridor_coverage),
            "p50": p50,
            "p75": p75,
            "p90": p90,
            "p95": p95,
            "p75_max": float(p75_max),
            "p90_max": float(p90_max),
            "ok": liquidity_state == "OK",
            "state": liquidity_state,
        }
        _finalize_meta(meta, quality_mode)
        return slice_df, meta

    def _qc_policy_id(self) -> str:
        policy_id = self.quality_cfg.get("policy_id")
        if not isinstance(policy_id, str) or not policy_id.strip():
            raise ValueError("option_chain quality.policy_id must be a non-empty string")
        return policy_id.strip()

    def _qc_cache_key(
        self,
        *,
        snapshot_data_ts: int,
        quality_mode: str,
        policy_id: str,
        tau_def: str,
        x_axis: str,
        atm_def: str,
        price_field: str,
        include_underlying: bool,
        tau_anchor_ts: int | None,
        market_ts_ref_method: str | None,
        qc_scope: str = "full_rows",
        row_policy_hash: str | None = None,
    ) -> tuple[Any, ...]:
        # policy_id must be bumped when QC thresholds/logic change to keep cache semantics correct.
        # include_underlying, tau_anchor_ts, and market_ts_ref_method are included because QC depends on them.
        # qc_scope and row_policy_hash ensure masked vs full QC never alias.
        return (
            int(snapshot_data_ts),
            str(quality_mode),
            str(policy_id),
            str(tau_def),
            str(x_axis),
            str(atm_def),
            str(price_field),
            bool(include_underlying),
            int(tau_anchor_ts) if tau_anchor_ts is not None else None,
            str(market_ts_ref_method) if market_ts_ref_method is not None else None,
            str(qc_scope),
            str(row_policy_hash) if row_policy_hash is not None else None,
        )

    def _qc_index_key(
        self,
        *,
        snapshot_data_ts: int,
        quality_mode: str,
        policy_id: str,
        tau_def: str,
        x_axis: str,
        atm_def: str,
        price_field: str,
        include_underlying: bool,
        qc_scope: str = "full_rows",
        row_policy_hash: str | None = None,
    ) -> tuple[Any, ...]:
        return (
            int(snapshot_data_ts),
            str(quality_mode),
            str(policy_id),
            str(tau_def),
            str(x_axis),
            str(atm_def),
            str(price_field),
            bool(include_underlying),
            str(qc_scope),
            str(row_policy_hash) if row_policy_hash is not None else None,
        )

    def _qc_index_key_from_full(self, key: tuple[Any, ...]) -> tuple[Any, ...]:
        return key[:10]

    def _cache_qc_report(
        self,
        meta: dict[str, Any],
        *,
        tau_def: str,
        x_axis: str,
        atm_def: str,
        price_field: str,
        qc_scope: str = "full_rows",
        row_policy_hash: str | None = None,
    ) -> dict[str, Any]:
        snapshot_data_ts = meta.get("snapshot_data_ts")
        policy_id = self._qc_policy_id()
        debug = bool(self.quality_cfg.get("qc_debug"))
        meta["policy_id"] = policy_id
        if snapshot_data_ts is None:
            return _qc_report_from_meta(meta, policy_id=policy_id, debug=debug)
        include_underlying = True
        key = self._qc_cache_key(
            snapshot_data_ts=int(snapshot_data_ts),
            quality_mode=str(meta.get("quality_mode") or ""),
            policy_id=policy_id,
            tau_def=str(tau_def),
            x_axis=str(x_axis),
            atm_def=str(atm_def),
            price_field=str(price_field),
            include_underlying=include_underlying,
            tau_anchor_ts=meta.get("tau_anchor_ts"),
            market_ts_ref_method=meta.get("market_ts_ref_method"),
            qc_scope=qc_scope,
            row_policy_hash=row_policy_hash,
        )
        cached = self._qc_cache.get(key)
        if cached is not None:
            return cached
        report = _qc_report_from_meta(meta, policy_id=policy_id, debug=debug)
        self._qc_cache[key] = report
        index_key = self._qc_index_key(
            snapshot_data_ts=int(snapshot_data_ts),
            quality_mode=str(meta.get("quality_mode") or ""),
            policy_id=policy_id,
            tau_def=str(tau_def),
            x_axis=str(x_axis),
            atm_def=str(atm_def),
            price_field=str(price_field),
            include_underlying=include_underlying,
            qc_scope=qc_scope,
            row_policy_hash=row_policy_hash,
        )
        self._qc_index[index_key] = key
        self._qc_keys_by_ts.setdefault(int(snapshot_data_ts), []).append(key)
        return report

    def _evicted_ts(self, evicted: object) -> int | None:
        # ts tag on evicted snapshot(s) for incremental eviction of derived caches;
        # supports various evicted object types for flexibility across cache implementations (e.g., raw snapshots, wrapped snapshots, lists of snapshots).
        if evicted is None:
            return None
        if isinstance(evicted, (int, np.integer)):
            return int(evicted)
        ts = getattr(evicted, "data_ts", None)
        if ts is None:
            ts = getattr(evicted, "timestamp", None)
        return int(ts) if ts is not None else None

    def _evict_from_push(self, evicted: object) -> None:
        ## Evict derived cache entries for snapshot(s) evicted from main cache on push.
        ## cache push may evict none, this ensures that we can handle all cases gracefully without errors and with best-effort eviction of derived caches.
        if evicted is None:
            return
        if isinstance(evicted, (list, tuple)):
            for item in evicted:
                ts = self._evicted_ts(item)
                if ts is not None:
                    self._evict_ts(int(ts))
            return
        ts = self._evicted_ts(evicted)
        if ts is not None:
            self._evict_ts(int(ts))

    def _evict_ts(self, ts: int) -> None:
        for key in self._market_keys_by_ts.pop(int(ts), []):
            self._market_cache.pop(key, None)
        for key in self._coords_aux_keys_by_ts.pop(int(ts), []):
            self._coords_aux_cache.pop(key, None)
        for key in self._select_tau_keys_by_ts.pop(int(ts), []):
            self._select_tau_cache.pop(key, None)
        for key in self._qc_keys_by_ts.pop(int(ts), []):
            self._qc_cache.pop(key, None)
            index_key = self._qc_index_key_from_full(key)
            if self._qc_index.get(index_key) == key:
                self._qc_index.pop(index_key, None)

    # ------------------------------------------------------------------
    # vNext selection + coord APIs
    # ------------------------------------------------------------------

    def coords_frame(
        self,
        ts: int | None = None,
        *,
        tau_def: str | None = None,
        x_axis: str | None = None,
        atm_def: str | None = None,
        price_field: str | None = None,
        quality_mode: str | None = None,
        drop_aux: bool | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Return a coordinate-mapped view derived from market_frame.

        Invariants: DataFrame includes market columns plus derived tau/x only; provenance lives in meta.
        This is a transient view (do not cache/mutate); only aux metadata may be cached for selection reuse.
        """

        snap = self.get_snapshot(ts)
        mode = _coerce_quality_mode(quality_mode or self.quality_mode)
        if snap is None:
            meta = _empty_meta(snapshot_data_ts=None, snapshot_market_ts=None, quality_mode=mode)
            _add_reason(meta, "MISSING_FRAME", _severity_for("MISSING_FRAME", mode, self.reason_severity), {})
            _finalize_meta(meta, mode)
            return pd.DataFrame(), meta
        tau_def = tau_def or self.coords_cfg.get("tau_def")
        x_axis = x_axis or self.coords_cfg.get("x_axis")
        atm_def = atm_def or self.coords_cfg.get("atm_def")
        price_field = price_field or self.coords_cfg.get("price_field")
        if tau_def is None or x_axis is None or atm_def is None or price_field is None:
            raise ValueError("option_chain coords config missing required fields")
        tau_def_s = str(tau_def)
        x_axis_s = str(x_axis)
        atm_def_s = str(atm_def)
        price_field_s = str(price_field)
        use_drop_aux = True if drop_aux is None else bool(drop_aux)
        coords_df, meta, aux = self._coords_frame_uncached(
            snap,
            tau_def=tau_def_s,
            x_axis=x_axis_s,
            atm_def=atm_def_s,
            price_field=price_field_s,
            quality_mode=mode,
            drop_aux=use_drop_aux,
        )
        # Cache aux data (selection helpers) keyed by snapshot + coordinate defs.
        # this is distinguished from aux_frame in the snapshot which is for exchange-provided auxiliary data; 
        # this aux is for derived metadata that may be useful for selection but is not needed for QC.
        aux_key = (int(snap.data_ts), tau_def_s, x_axis_s, atm_def_s, price_field_s, use_drop_aux, mode)
        if aux_key not in self._coords_aux_cache:
            self._coords_aux_cache[aux_key] = aux
            self._coords_aux_keys_by_ts.setdefault(int(snap.data_ts), []).append(aux_key)
        return coords_df, meta

    def qc_report(
        self,
        ts: int | None = None,
        *,
        tau_def: str | None = None,
        x_axis: str | None = None,
        atm_def: str | None = None,
        price_field: str | None = None,
        quality_mode: str | None = None,
    ) -> dict[str, Any]:
        """Return a cached QC report for a snapshot and coordinate definition.

        Invariants: report is JSON-friendly and derived from meta; no frame columns are used.
        Cache hits must not trigger pandas computation; misses compute QC via coords_frame.
        """
        mode = _coerce_quality_mode(quality_mode or self.quality_mode)
        tau_def = tau_def or self.coords_cfg.get("tau_def")
        x_axis = x_axis or self.coords_cfg.get("x_axis")
        atm_def = atm_def or self.coords_cfg.get("atm_def")
        price_field = price_field or self.coords_cfg.get("price_field")
        if tau_def is None or x_axis is None or atm_def is None or price_field is None:
            raise ValueError("option_chain coords config missing required fields")
        tau_def_s = str(tau_def)
        x_axis_s = str(x_axis)
        atm_def_s = str(atm_def)
        price_field_s = str(price_field)

        snap = self.get_snapshot(ts)
        if snap is not None:
            policy_id = self._qc_policy_id()
            _rph = snap.row_policy_hash
            scopes = ["masked_rows", "full_rows"] if snap.row_mask is not None else ["full_rows"]
            for _qc_scope in scopes:
                index_key = self._qc_index_key(
                    snapshot_data_ts=int(snap.data_ts),
                    quality_mode=mode,
                    policy_id=policy_id,
                    tau_def=tau_def_s,
                    x_axis=x_axis_s,
                    atm_def=atm_def_s,
                    price_field=price_field_s,
                    include_underlying=True,
                    qc_scope=_qc_scope,
                    row_policy_hash=_rph,
                )
                full_key = self._qc_index.get(index_key)
                if full_key is not None:
                    cached = self._qc_cache.get(full_key)
                    if cached is not None:
                        return cached

        _, meta = self.coords_frame(
            ts,
            tau_def=tau_def_s,
            x_axis=x_axis_s,
            atm_def=atm_def_s,
            price_field=price_field_s,
            quality_mode=mode,
        )
        return self._cache_qc_report(
            meta,
            tau_def=tau_def_s,
            x_axis=x_axis_s,
            atm_def=atm_def_s,
            price_field=price_field_s,
            qc_scope=meta.get("qc_scope", "full_rows"),
            row_policy_hash=meta.get("row_policy_hash"),
        )

    def select_tau(
        self,
        ts: int | None = None,
        *,
        tau_ms: int,
        method: str | None = None,
        quality_mode: str | None = None,
        tau_def: str | None = None,
        term_bucket_ms: int | None = None,
        max_bucket_hops: int | None = None,
        coords_df: pd.DataFrame | None = None,
        base_meta: dict[str, Any] | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        # Select expiry candidate(s) for a target tau in the current snapshot (optionally bracket+weights), 
        # using term-bucket prefilter + cached per-snapshot expiry->tau map, and return the sliced coords + auditable selection meta.
        mode = _coerce_quality_mode(quality_mode or self.quality_mode)
        method = method or self.selection_cfg.get("method")
        tau_def = tau_def or self.coords_cfg.get("tau_def")
        if method is None or tau_def is None:
            raise ValueError("option_chain selection config missing required fields")
        method_s = str(method)
        if method_s not in {"nearest_bucket", "bracket"}:
            raise ValueError(f"option_chain selection.method unsupported: {method_s}")
        tau_def_s = str(tau_def)
        tb = int(term_bucket_ms) if term_bucket_ms is not None else int(self.term_bucket_ms) # 颗粒度
        hops = int(max_bucket_hops) if max_bucket_hops is not None else int(self.quality_cfg["max_bucket_hops"]) # 在没有满足条件的bucket时，向外扩散的最大bucket数量
        x_axis = self.coords_cfg.get("x_axis")
        atm_def = self.coords_cfg.get("atm_def")
        price_field = self.coords_cfg.get("price_field")
        
        use_drop_aux = True  # must match coords_frame drop_aux for aux_key lookup
        if coords_df is None or base_meta is None:
            coords_df, base_meta = self.coords_frame(
                ts,
                tau_def=tau_def_s,
                x_axis=x_axis,
                atm_def=atm_def,
                price_field=price_field,
                quality_mode=mode,
                drop_aux=use_drop_aux,
            )
        meta = _clone_meta(base_meta)
        _set_selection_context(meta, method=method_s, tau_ms=int(tau_ms), term_bucket_ms=tb, tau_def=tau_def_s)
        if coords_df is None or coords_df.empty:
            _add_reason(meta, "EMPTY_CHAIN", _severity_for("EMPTY_CHAIN", mode, self.reason_severity), {})
            _finalize_meta(meta, mode)
            return pd.DataFrame(), meta

        snap_ts = meta.get("snapshot_data_ts")
        if snap_ts is None:
            snap_ts = int(getattr(self.get_snapshot(ts), "data_ts", 0) or 0)
        liquidity_sig = self._liquidity_gate_signature(row_policy_hash=meta.get("row_policy_hash"))
        cache_key = (int(snap_ts), int(tau_ms), method_s, tb, int(hops), mode, tau_def_s, *liquidity_sig)  # include liquidity + row-policy in cache semantics
        cached_sel = self._select_tau_cache.get(cache_key)
        if cached_sel is not None:
            slice_df, out_meta = _apply_selection_slice(
                coords_df,
                meta,
                cached_sel,
                mode,
                min_n_per_slice=self._min_n_per_slice(),
                reason_severity=self.reason_severity,
            )
            return self._apply_selection_liquidity_gate(slice_df, out_meta, quality_mode=mode, ts=ts)
        
        # Scenario: cache miss on expiry selection; recompute from current coordinates.
        x_axis_s = str(x_axis)
        atm_def_s = str(atm_def)
        price_field_s = str(price_field)
        aux_key = (int(snap_ts), tau_def_s, x_axis_s, atm_def_s, price_field_s, use_drop_aux, mode)
        aux = self._coords_aux_cache.get(aux_key, {})
        expiry_tau = aux.get("expiry_tau")

        # Why: aux cache can be absent after eviction/restart, so expiry->tau map must be derivable from coords_df.
        if expiry_tau is None:
            tau_anchor_ts = meta.get("tau_anchor_ts")
            tau_series = _compute_tau_series(
                coords_df,
                tau_anchor_ts=int(tau_anchor_ts) if tau_anchor_ts is not None else None,
            )
            expiry_tau = tau_series.groupby(coords_df["expiry_ts"]).median()

        expiry_tau = pd.to_numeric(expiry_tau, errors="coerce").dropna().round().astype("int64")
        if expiry_tau.empty:
            _add_reason(
                meta,
                "EXPIRY_SELECTION_AMBIGUOUS",
                _severity_for("EXPIRY_SELECTION_AMBIGUOUS", mode, self.reason_severity),
                {},
            )
            _finalize_meta(meta, mode)
            return pd.DataFrame(), meta

        target_tau = int(tau_ms)
        bucket_key = (target_tau // tb) * tb if tb > 0 else 0
        bucketed = ((expiry_tau // tb) * tb) if tb > 0 else expiry_tau * 0
        candidate_mask = bucketed == bucket_key
        if not bool(candidate_mask.any()):
            # Scenario: if no expiry lands in the target bucket, widen search by neighbor buckets up to `hops`.
            if hops > 0 and tb > 0:
                for hop in range(1, hops + 1):
                    lower = bucket_key - hop * tb
                    upper = bucket_key + hop * tb
                    candidate_mask = (bucketed == lower) | (bucketed == upper)
                    if bool(candidate_mask.any()):
                        break
        candidates = expiry_tau[candidate_mask] if bool(candidate_mask.any()) else expiry_tau

        selected_expiries: list[int] = []
        weights: list[float] = []
        if method_s == "bracket":
            # Role: choose lower/upper expiries around target tau and weight by linear distance.
            below = candidates[candidates <= target_tau]
            above = candidates[candidates >= target_tau]
            lower_tau = below.max() if not below.empty else None
            upper_tau = above.min() if not above.empty else None
            if lower_tau is None and upper_tau is None:
                _add_reason(
                    meta,
                    "EXPIRY_SELECTION_AMBIGUOUS",
                    _severity_for("EXPIRY_SELECTION_AMBIGUOUS", mode, self.reason_severity),
                    {},
                )
                _finalize_meta(meta, mode)
                return pd.DataFrame(), meta
            if lower_tau is None:
                selected_expiries = [int(above.idxmin())]
                weights = [1.0]
            elif upper_tau is None:
                selected_expiries = [int(below.idxmax())]
                weights = [1.0]
            else:
                lower_expiry = int(below.idxmax())
                upper_expiry = int(above.idxmin())
                if upper_expiry == lower_expiry or int(upper_tau) == int(lower_tau):
                    selected_expiries = [lower_expiry]
                    weights = [1.0]
                else:
                    # Invariant: bracket weights are normalized to [0,1] and sum to 1.
                    w_upper = (target_tau - int(lower_tau)) / float(int(upper_tau) - int(lower_tau))
                    w_upper = max(0.0, min(1.0, w_upper))
                    w_lower = 1.0 - w_upper
                    selected_expiries = [lower_expiry, upper_expiry]
                    weights = [w_lower, w_upper]

        elif method_s == "nearest_bucket":
            # Role: pick the single expiry with minimum absolute tau distance.
            nearest = (candidates - target_tau).abs()
            expiry = int(nearest.idxmin())
            selected_expiries = [expiry]
            weights = [1.0]
        else:
            raise ValueError(f"option_chain selection.method unsupported: {method_s}")

        tau_errors = [abs(int(expiry_tau.loc[ex]) - target_tau) for ex in selected_expiries]
        max_tau_error_ms = int(self.quality_cfg["max_tau_error_ms"])
        if tau_errors and max(tau_errors) > max_tau_error_ms:
            _add_reason(
                meta,
                "EXPIRY_SELECTION_AMBIGUOUS",
                _severity_for("EXPIRY_SELECTION_AMBIGUOUS", mode, self.reason_severity),
                {"max_tau_error_ms": max_tau_error_ms, "tau_error_ms": max(tau_errors)},
            )

        selection = {
            "selected_expiries": selected_expiries,
            "weights": weights,
            "tau_target_ms": int(target_tau),
        }
        if cache_key not in self._select_tau_cache:
            self._select_tau_cache[cache_key] = selection
            # Invariant: eviction index uses the same cache key tuple used for lookup.
            self._select_tau_keys_by_ts.setdefault(int(snap_ts), []).append(cache_key)
        slice_df, out_meta = _apply_selection_slice(
            coords_df,
            meta,
            selection,
            mode,
            min_n_per_slice=self._min_n_per_slice(),
            reason_severity=self.reason_severity,
        )
        return self._apply_selection_liquidity_gate(slice_df, out_meta, quality_mode=mode, ts=ts)

    def select_point(
        self,
        ts: int | None = None,
        *,
        tau_ms: int,
        x: float,
        x_axis: str | None = None,
        method: str | None = None,
        interp: str | None = None,
        cp_policy: str | None = None,
        quality_mode: str | None = None,
        price_field: str | None = None,
        atm_def: str | None = None,
        tau_def: str | None = None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        # Role: select/interpolate one (tau,x) point and always return row payload + provenance meta.
        cp_policy = cp_policy or self.coords_cfg.get("cp_policy")
        if cp_policy not in {"same", "either"}:
            cp_policy = "same"
        method = method or self.selection_cfg.get("method")
        interp = interp or self.selection_cfg.get("interp")
        if interp is None:
            raise ValueError("option_chain selection config missing interp")
        x_axis = x_axis or self.coords_cfg.get("x_axis")
        price_field = price_field or self.coords_cfg.get("price_field")
        atm_def = atm_def or self.coords_cfg.get("atm_def")
        tau_def = tau_def or self.coords_cfg.get("tau_def")
        if method is None or interp is None or x_axis is None or price_field is None or atm_def is None or tau_def is None:
            raise ValueError("option_chain selection/coords config missing required fields")
        method_s = str(method)
        interp_s = str(interp)
        x_axis_s = str(x_axis)
        price_field_s = str(price_field)
        atm_def_s = str(atm_def)
        tau_def_s = str(tau_def)
        mode = _coerce_quality_mode(quality_mode or self.quality_mode)
        coords_df, meta = self.coords_frame(
            ts,
            tau_def=tau_def_s,
            x_axis=x_axis_s,
            atm_def=atm_def_s,
            price_field=price_field_s,
            quality_mode=mode,
        )
        if coords_df is None or coords_df.empty:
            _add_reason(meta, "EMPTY_CHAIN", _severity_for("EMPTY_CHAIN", mode, self.reason_severity), {})
            _finalize_meta(meta, mode)
            return None, meta
        tau_slice, meta_tau = self.select_tau(
            ts,
            tau_ms=int(tau_ms),
            method=method_s,
            quality_mode=mode,
            tau_def=tau_def_s,
            coords_df=coords_df,
            base_meta=meta,
        )
        meta = _merge_meta(meta, meta_tau)
        if tau_slice is None or tau_slice.empty:
            _add_reason(
                meta,
                "COVERAGE_LOW",
                _severity_for("COVERAGE_LOW", mode, self.reason_severity),
                {"min_n": self._min_n_per_slice()},
            )
            _finalize_meta(meta, mode)
            return None, meta
        snap_ts = int(meta["snapshot_data_ts"])
        tau_anchor_ts = int(meta["tau_anchor_ts"])
        atm_ref = meta.get("atm_ref")
        point, meta_point = _select_point_from_slice(
            tau_slice,
            x=float(x),
            x_axis=x_axis_s,
            interp=interp_s,
            price_field=price_field_s,
            cp_policy=cp_policy,
            snapshot_data_ts=snap_ts,
            tau_target_ms=int(tau_ms),
            tau_anchor_ts=tau_anchor_ts,
            atm_ref=atm_ref,
            quality_mode=mode,
        )
        meta = _merge_meta(meta, meta_point)
        _finalize_meta(meta, mode)
        return point, meta

    def available_terms(
        self,
        ts: int | None = None,
        *,
        term_bucket_ms: int | None = None,
        quality_mode: str | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        mode = _coerce_quality_mode(quality_mode or self.quality_mode)
        tb = int(term_bucket_ms) if term_bucket_ms is not None else int(self.term_bucket_ms)
        coords_df, meta = self.coords_frame(ts, quality_mode=mode)
        if coords_df is None or coords_df.empty:
            _add_reason(meta, "EMPTY_CHAIN", _severity_for("EMPTY_CHAIN", mode, self.reason_severity), {})
            _finalize_meta(meta, mode)
            return pd.DataFrame(), meta
        grouped = coords_df.groupby("expiry_ts")
        meta_tau_anchor_ts = meta.get("tau_anchor_ts")
        tau_series = _compute_tau_series(
            coords_df,
            tau_anchor_ts=int(meta_tau_anchor_ts) if meta_tau_anchor_ts is not None else None,
        )
        rows = []
        staleness_meta = meta.get("staleness") if isinstance(meta.get("staleness"), dict) else {}
        staleness_ms = staleness_meta.get("staleness_ms") if staleness_meta else 0
        for expiry_ts, group in grouped:
            group_tau = tau_series.loc[group.index]
            tau_val = group_tau.dropna().median() if not group_tau.empty else 0
            tau_ms = int(tau_val) if pd.notna(tau_val) else 0
            term_key = (tau_ms // tb) * tb if tb > 0 else 0
            expiry_val = _to_int_scalar(expiry_ts)
            rows.append(
                {
                    "expiry_ts": expiry_val,
                    "tau_ms": int(tau_ms),
                    "term_key_ms": int(term_key),
                    "n_contracts": int(len(group)),
                    "coverage_ratio": 1.0 if len(group) > 0 else 0.0,
                    "tradable_ratio": 1.0 if meta.get("tradable") else 0.0,
                    "staleness_ms": int(staleness_ms) if staleness_ms is not None else None,
                }
            )
        df = pd.DataFrame(rows)
        for col in ("expiry_ts", "tau_ms", "term_key_ms", "staleness_ms"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        _finalize_meta(meta, mode)
        return df, meta

    def resolve_underlying(
        self,
        ts: int | None = None,
        *,
        field: str | None = None,
        quality_mode: str | None = None,
    ) -> tuple[float | None, dict[str, Any]]:
        field = field or str(self.coords_cfg.get("underlying_field"))
        mode = _coerce_quality_mode(quality_mode or self.quality_mode)
        snap = self.get_snapshot(ts)
        if snap is None:
            meta = _empty_meta(snapshot_data_ts=None, snapshot_market_ts=None, quality_mode=mode)
            _add_reason(meta, "MISSING_FRAME", _severity_for("MISSING_FRAME", mode, self.reason_severity), {})
            _finalize_meta(meta, mode)
            return None, meta
        df = snap.frame
        value = _resolve_underlying(df, field)
        market_ts, market_ts_ref_method = _market_ts_ref_info(df, snap, method=str(self.market_ts_ref_method))
        meta = _empty_meta(snapshot_data_ts=int(snap.data_ts), snapshot_market_ts=market_ts, quality_mode=mode)
        meta["market_ts_ref_method"] = market_ts_ref_method
        meta["reference"] = {"field": field}
        if value is None:
            _add_reason(
                meta,
                "MISSING_UNDERLYING_REF",
                _severity_for("MISSING_UNDERLYING_REF", mode, self.reason_severity),
                {"field": field},
            )
        _finalize_meta(meta, mode)
        return value, meta

    def resolve_atm(
        self,
        ts: int | None = None,
        *,
        atm_def: str | None = None,
        underlying_field: str | None = None,
        quality_mode: str | None = None,
    ) -> tuple[float | None, dict[str, Any]]:
        atm_def = atm_def or str(self.coords_cfg.get("atm_def"))
        field = underlying_field or atm_def
        value, meta = self.resolve_underlying(ts, field=field, quality_mode=quality_mode)
        meta["reference"] = {"atm_def": atm_def, "field": field}
        return value, meta

    def window_for_term(
        self,
        term_key_ms: int,
        n: int = 1,
        *,
        ts: int | None = None,
        annotate: bool = True,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        ts = self._anchor_ts if ts is None and self._anchor_ts is not None else ts
        base_ts = self.last_timestamp() if ts is None else int(ts)
        if base_ts is None:
            meta = _empty_meta(snapshot_data_ts=None, snapshot_market_ts=None, quality_mode=self.quality_mode)
            _add_reason(meta, "MISSING_FRAME", _severity_for("MISSING_FRAME", self.quality_mode, self.reason_severity), {})
            _finalize_meta(meta, self.quality_mode)
            return pd.DataFrame(), meta
        frames: list[pd.DataFrame] = []
        meta = _empty_meta(snapshot_data_ts=base_ts, snapshot_market_ts=None, quality_mode=self.quality_mode)
        if hasattr(self.cache, "window_for_term"):
            for snap in self.cache.window_for_term(int(term_key_ms), int(n)):  # type: ignore[attr-defined]
                if not annotate:
                    frames.append(snap.frame)
                else:
                    view = OptionChainSnapshotView.for_term_bucket(
                        base=snap,
                        term_key_ms=int(term_key_ms),
                        term_bucket_ms=int(self.term_bucket_ms),
                    )
                    frames.append(view.frame)
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        _finalize_meta(meta, self.quality_mode)
        return out, meta

    def snapshot_view(
        self,
        *,
        kind: str,
        key: int,
        ts: int | None = None,
        annotate: bool = True,
    ) -> OptionChainSnapshotView | None:
        snap = self.get_snapshot(ts)
        if snap is None:
            return None
        if kind == "expiry":
            view = OptionChainSnapshotView.for_expiry(base=snap, expiry_ts=int(key))
        elif kind == "term_bucket":
            view = OptionChainSnapshotView.for_term_bucket(
                base=snap,
                term_key_ms=int(key),
                term_bucket_ms=int(self.term_bucket_ms),
            )
        else:
            return OptionChainSnapshotView(
                base=snap,
                frame_filter=lambda f: f,
                slice_kind=str(kind),
                slice_key=int(key),
            )
        return view if annotate else OptionChainSnapshotView(base=snap, frame_filter=lambda f: f)

    def window_for_tau(
        self,
        ts_start: int,
        ts_end: int,
        *,
        tau_ms: int,
        step_ms: int,
        method: str | None = None,
        quality_mode: str | None = None,
    ) -> list[OptionChainSnapshotView]:
        # tau: time-to-expiry, tau_ms = expiry_ts - anchor_ts
        # Return a list of snapshot views sliced by select_tau for a given tau_ms across a time window [ts_start, ts_end] with step step_ms.
        mode = _coerce_quality_mode(quality_mode or self.quality_mode)
        method = method or self.selection_cfg.get("method")
        out: list[OptionChainSnapshotView] = [] 
        for ts in _iter_ts(int(ts_start), int(ts_end), int(step_ms)):
            snap = self.get_snapshot(ts)
            if snap is None:
                continue
            coords_df, meta = self.select_tau(ts, tau_ms=int(tau_ms), method=method, quality_mode=mode)
            selection = meta.get("selection") or {}
            selected = selection.get("selected_expiries") or []
            view = OptionChainSnapshotView(
                base=snap,
                frame_filter=lambda f, xs=selected: f.loc[f["expiry_ts"].isin(xs)] if xs else _empty_df_like(f),
                slice_kind="tau",
                slice_key=int(tau_ms),
                selection=selection,
            ) 
            # 实际上就是保存了一个frame filter，这个filter是一个lambda函数把区间内的expiry_ts筛选出来；当调用snapshot_view方法时会把这个filter应用到对应的snapshot上，得到一个切片。
            out.append(view)
        return out

    def track_tau_point(
        self,
        *,
        tau_ms: int,
        x: float,
        ts_start: int,
        ts_end: int,
        step_ms: int,
        x_axis: str | None = None,
        method: str | None = None,
        interp: str | None = None,
        quality_mode: str | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        # Track the selected (tau,x) point over time for a given tau_ms and x coordinate,
        # returning a DataFrame of the point's history plus meta summary including roll count, 
        # missing steps, and max tau error.
        mode = _coerce_quality_mode(quality_mode or self.quality_mode)
        method = method or self.selection_cfg.get("method")
        interp = interp or self.selection_cfg.get("interp")  # interp is for 
        x_axis = x_axis or self.coords_cfg.get("x_axis")
        rows: list[dict[str, Any]] = []
        roll_count = 0
        missing = 0
        prev_expiry = None
        first_hard_ts = None
        max_error = 0
        for ts in _iter_ts(int(ts_start), int(ts_end), int(step_ms)):
            point, meta = self.select_point(
                ts,
                tau_ms=int(tau_ms),
                x=float(x),
                x_axis=x_axis,
                method=method,
                interp=interp,
                quality_mode=mode,
            )
            expiry_ts = point.get("expiry_ts") if point else None
            tau_realized = point.get("tau_realized_ms") if point else None
            tau_error = abs(int(tau_realized) - int(tau_ms)) if tau_realized is not None else None
            if expiry_ts is not None and prev_expiry is not None and int(expiry_ts) != int(prev_expiry):
                roll_count += 1
            if expiry_ts is not None:
                prev_expiry = int(expiry_ts)
            if tau_error is not None:
                max_error = max(max_error, int(tau_error))
            if meta.get("state") == "HARD_FAIL" and first_hard_ts is None:
                first_hard_ts = int(ts)
            if point is None:
                missing += 1
            selection = meta.get("selection") or {}
            selected_expiries = selection.get("selected_expiries") or []
            weights = selection.get("weights") or []
            selection_weight = None
            if expiry_ts is not None and selected_expiries and weights:
                try:
                    idx = selected_expiries.index(int(expiry_ts))
                    selection_weight = float(weights[idx]) if idx < len(weights) else None
                except Exception:
                    selection_weight = None
            missing_reason = None
            if point is None and meta.get("reasons"):
                missing_reason = meta["reasons"][0].get("reason_code")
            rows.append(
                {
                    "ts": int(ts),
                    "expiry_ts_used": expiry_ts,
                    "tau_ms_target": int(tau_ms),
                    "tau_ms_actual": tau_realized,
                    "selection_weight": selection_weight,
                    "quality_state": meta.get("state"),
                    "missing_reason": missing_reason,
                    "tau_target_ms": int(tau_ms),
                    "expiry_ts": expiry_ts,
                    "tau_realized_ms": tau_realized,
                    "tau_error_ms": tau_error,
                    "x": float(x),
                    "state": meta.get("state"),
                    "tradable": meta.get("tradable"),
                }
            )
        meta_out = {
            "roll_count": roll_count,
            "missing_steps": missing,
            "max_tau_error_ms": max_error,
            "first_hard_fail_ts": first_hard_ts,
        }
        out = pd.DataFrame(rows)
        for col in (
            "ts",
            "expiry_ts_used",
            "tau_ms_target",
            "tau_ms_actual",
            "tau_target_ms",
            "expiry_ts",
            "tau_realized_ms",
            "tau_error_ms",
        ):
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
        return out, meta_out

    def track_expiry_point(
        self,
        *,
        expiry_ts: int,
        x: float,
        ts_start: int,
        ts_end: int,
        step_ms: int,
        x_axis: str | None = None,
        interp: str | None = None,
        quality_mode: str | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        mode = _coerce_quality_mode(quality_mode or self.quality_mode)
        interp = interp or self.selection_cfg.get("interp")
        rows: list[dict[str, Any]] = []
        x_axis = x_axis or self.coords_cfg.get("x_axis")
        assert x_axis is not None
        missing = 0
        first_hard_ts = None
        for ts in _iter_ts(int(ts_start), int(ts_end), int(step_ms)):
            coords_df, meta = self.coords_frame(ts, quality_mode=mode)
            if coords_df is None or coords_df.empty:
                missing += 1
                rows.append(
                    {
                        "ts": int(ts),
                        "expiry_ts": int(expiry_ts),
                        "tau_realized_ms": None,
                        "x": float(x),
                        "state": meta.get("state"),
                        "tradable": meta.get("tradable"),
                    }
                )
                continue
            slice_df = coords_df.loc[coords_df["expiry_ts"] == int(expiry_ts)]
            snap_ts = int(meta["snapshot_data_ts"])
            tau_anchor_ts = int(meta["tau_anchor_ts"])
            atm_ref = meta.get("atm_ref")
            point, meta_point = _select_point_from_slice(
                slice_df,
                x=float(x),
                x_axis=x_axis,
                interp=str(interp),
                price_field="mark_price",
                cp_policy=str(self.coords_cfg.get("cp_policy")),
                snapshot_data_ts=snap_ts,
                tau_target_ms=0,
                tau_anchor_ts=tau_anchor_ts,
                atm_ref=atm_ref,
                quality_mode=mode,
            )
            meta = _merge_meta(meta, meta_point)
            if meta.get("state") == "HARD_FAIL" and first_hard_ts is None:
                first_hard_ts = int(ts)
            if point is None:
                missing += 1
            rows.append(
                {
                    "ts": int(ts),
                    "expiry_ts": int(expiry_ts),
                    "tau_realized_ms": point.get("tau_realized_ms") if point else None,
                    "x": float(x),
                    "state": meta.get("state"),
                    "tradable": meta.get("tradable"),
                }
            )
        meta_out = {"missing_steps": missing, "first_hard_fail_ts": first_hard_ts}
        out = pd.DataFrame(rows)
        for col in ("ts", "expiry_ts", "tau_realized_ms"):
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
        return out, meta_out

    def track_point(self, **kwargs: Any) -> tuple[pd.DataFrame, dict[str, Any]]:
        return self.track_tau_point(**kwargs)

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

    def _maybe_backfill(self, *, target_ts: int) -> None:
        if not self._should_backfill():
            return
        self._backfill_to_target(target_ts=int(target_ts))

    # ------------------------------------------------------------------
    # Backfill helpers (realtime/mock only)
    # ------------------------------------------------------------------

    def _should_backfill(self) -> bool:
        # Backfill policy: option_chain is live-only streaming and must never historical-backfill.
        return False

    def _bootstrap_from_files(self, *, anchor_ts: int, lookback: Any | None) -> None:
        bars = _coerce_lookback_bars(lookback, self.interval_ms, getattr(self.cache, "maxlen", None))
        if bars is None or bars <= 0 or self.interval_ms is None:
            return
        start_ts = int(anchor_ts) - (int(bars) - 1) * int(self.interval_ms)
        end_ts = int(anchor_ts)
        log_info(
            self._logger,
            "option_chain.bootstrap.start",
            symbol=self.display_symbol, instrument_symbol=self.symbol,
            asset=self.asset,
            start_ts=start_ts,
            end_ts=end_ts,
            bars=int(bars),
        )
        prev_anchor = self._anchor_ts
        if prev_anchor is None:
            self._anchor_ts = int(anchor_ts)
        try:
            loaded = self._load_from_files(start_ts=start_ts, end_ts=end_ts)
            log_info(
                self._logger,
                "option_chain.bootstrap.done",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                loaded_count=int(loaded),
                cache_size=len(getattr(self.cache, "buffer", [])),
            )
        except Exception as exc:
            log_warn(
                self._logger,
                "option_chain.bootstrap.error",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                err_type=type(exc).__name__,
                err=str(exc),
            )
        finally:
            if prev_anchor is None:
                self._anchor_ts = prev_anchor

    def _backfill_to_target(self, *, target_ts: int) -> None:
        if self.interval_ms is None or self.interval_ms <= 0:
            return
        last_ts = self.last_timestamp()
        if last_ts is None:
            lookback = self.bootstrap_cfg.get("lookback") if self.bootstrap_cfg else None
            bars = _coerce_lookback_bars(lookback, self.interval_ms, getattr(self.cache, "maxlen", None))
            if bars is None or bars <= 0:
                throttle_id = throttle_key("option_chain.backfill.no_lookback", type(self).__name__, self.symbol, self.interval_ms)
                if log_throttle(throttle_id, 60.0):
                    log_warn(
                        self._logger,
                        "option_chain.backfill.no_lookback",
                        symbol=self.display_symbol, instrument_symbol=self.symbol,
                        asset=self.asset,
                        target_ts=int(target_ts),
                    )
                return
            start_ts = int(target_ts) - (int(bars) - 1) * int(self.interval_ms)
            end_ts = int(target_ts)
            log_warn(
                self._logger,
                "option_chain.backfill.cold_start",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                target_ts=int(target_ts),
                interval_ms=int(self.interval_ms),
                start_ts=start_ts,
                end_ts=end_ts,
            )
            try:
                loaded = self._backfill_from_source(start_ts=start_ts, end_ts=end_ts, target_ts=target_ts)
                log_info(
                    self._logger,
                    "option_chain.backfill.done",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    asset=self.asset,
                    loaded_count=int(loaded),
                    cache_size=len(getattr(self.cache, "buffer", [])),
                )
            except Exception as exc:
                log_exception(
                    self._logger,
                    "option_chain.backfill.error",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    asset=self.asset,
                    err=str(exc),
                )
            return
        # Gap check is a guard: no downstream updates until data is continuous.
        gap_threshold = int(target_ts) - int(self.interval_ms)
        if int(last_ts) >= gap_threshold:
            return
        start_ts = int(last_ts) + int(self.interval_ms)
        end_ts = int(target_ts)
        log_warn(
            self._logger,
            "option_chain.gap_detected",
            symbol=self.display_symbol, instrument_symbol=self.symbol,
            asset=self.asset,
            last_ts=int(last_ts),
            target_ts=int(target_ts),
            interval_ms=int(self.interval_ms),
            start_ts=start_ts,
            end_ts=end_ts,
        )
        try:
            loaded = self._backfill_from_source(start_ts=start_ts, end_ts=end_ts, target_ts=target_ts)
            log_info(
                self._logger,
                "option_chain.backfill.done",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                loaded_count=int(loaded),
                cache_size=len(getattr(self.cache, "buffer", [])),
            )
            post_last = self.last_timestamp()
            if post_last is None or int(post_last) < (int(target_ts) - int(self.interval_ms)):
                log_warn(
                    self._logger,
                    "option_chain.backfill.incomplete",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    asset=self.asset,
                    last_ts=int(post_last) if post_last is not None else None,
                    target_ts=int(target_ts),
                    interval_ms=int(self.interval_ms),
                )
        except Exception as exc:
            log_exception(
                self._logger,
                "option_chain.backfill.error",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                asset=self.asset,
                err=str(exc),
            )

    def _load_from_files(self, *, start_ts: int, end_ts: int) -> int:
        stage = "cleaned"
        if self._engine_mode == EngineMode.SAMPLE:
            stage = "sample"
        paths = resolve_cleaned_paths(
            data_root=self._data_root,
            stage=stage,
            domain="option_chain",
            asset=self.asset,
            interval=self.interval,
            start_ts=int(start_ts),
            end_ts=int(end_ts),
        )
        if not paths:
            return 0
        source = OptionChainFileSource(
            root=f"{stage}/option_chain",
            asset=self.asset,
            interval=self.interval,
            start_ts=int(start_ts),
            end_ts=int(end_ts),
            paths=paths,
        )
        last_ts: int | None = None
        last_snap = self.cache.last()
        if last_snap is not None:
            last_ts = int(last_snap.data_ts)
        count = 0
        for payload in source:
            ts = _infer_data_ts(payload)
            if last_ts is not None and int(ts) <= int(last_ts):
                continue
            self.on_new_tick(_tick_from_payload(payload, symbol=self.display_symbol, source_id=getattr(self, "source_id", None)))
            last_ts = int(ts)
            count += 1
        return count

    def _backfill_from_source(self, *, start_ts: int, end_ts: int, target_ts: int) -> int:
        worker = self._backfill_worker
        if worker is None:
            throttle_id = throttle_key("option_chain.backfill.no_worker", type(self).__name__, self.symbol, self.interval_ms)
            if log_throttle(throttle_id, 60.0):
                log_debug(
                    self._logger,
                    "option_chain.backfill.no_worker",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    asset=self.asset,
                    start_ts=int(start_ts),
                    end_ts=int(end_ts),
                )
            return 0
        backfill = getattr(worker, "backfill", None)
        if not callable(backfill):
            throttle_id = throttle_key("option_chain.backfill.no_worker_method", type(self).__name__, self.symbol, self.interval_ms)
            if log_throttle(throttle_id, 60.0):
                log_debug(
                    self._logger,
                    "option_chain.backfill.no_worker_method",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    worker_type=type(worker).__name__,
                )
            return 0
        emit = self._backfill_emit or self.on_new_tick
        return int(
            cast(int, backfill(
                start_ts=int(start_ts),
                end_ts=int(end_ts),
                anchor_ts=int(target_ts),
                emit=emit,
            ))
        )
