from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, cast

import pandas as pd

from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler, to_interval_ms
from quant_engine.data.contracts.snapshot import (
    MarketSpec,
    ensure_market_spec,
    merge_market_spec,
    classify_gap,
)
from quant_engine.utils.logger import get_logger, log_debug, log_info, log_warn, log_exception
from quant_engine.runtime.modes import EngineMode
from quant_engine.utils.cleaned_path_resolver import resolve_cleaned_paths, base_asset_from_symbol
from quant_engine.utils.paths import resolve_data_root
from ingestion.contracts.tick import IngestionTick, _coerce_epoch_ms

from .cache import OptionTradesBucketedCache
from .snapshot import OptionTradeEvent


class OptionTradesDataHandler(RealTimeDataHandler):
    """Runtime Option-Trades handler (mode-agnostic).

    Domain semantics:
      - Stores immutable OptionTradeEvent (event-time facts)
      - Event time is trade timestamp (epoch ms) -> event.data_ts
      - No system time / latency stored in events

    Read semantics (anti-lookahead):
      - align_to(ts) sets an anchor; all reads clamp to min(ts, anchor)
      - cache ordering is by data_ts (event time)

    Polling cadence is external to this handler; it does not own IO.

    Config mapping (Strategy.DATA.*.option_trades):
      - source: origin identifier (e.g., "DERIBIT") (kept for metadata/routing)
      - cache.maxlen: global cache maxlen
      - cache.per_term_maxlen: per-term bucket maxlen (term := expiry_ts - data_ts)
      - cache.term_bucket_ms: term bucket width in ms (default: 1 day)
      - columns: dataframe view columns (core or aux keys)
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
    cache: OptionTradesBucketedCache

    _anchor_ts: int | None
    _logger: Any
    _backfill_worker: Any | None
    _backfill_emit: Any | None
    _engine_mode: EngineMode | None
    _data_root: Any

    def __init__(self, symbol: str, **kwargs: Any):
        self.symbol = symbol
        self.source = str(kwargs.get("source") or "DERIBIT")

        interval = kwargs.get("interval")
        if interval is not None and (not isinstance(interval, str) or not interval):
            raise ValueError("option_trades 'interval' must be a non-empty string if provided")
        self.interval = interval
        interval_ms = to_interval_ms(self.interval) if self.interval is not None else None
        self.interval_ms = int(interval_ms) if interval_ms is not None else None
        self.asset = base_asset_from_symbol(str(kwargs.get("asset") or kwargs.get("currency") or self.symbol))

        cache = kwargs.get("cache") or {}
        if not isinstance(cache, dict):
            raise TypeError("option_trades 'cache' must be a dict")
        self.cache_cfg = dict(cache)
        self.bootstrap_cfg = kwargs.get("bootstrap") or None
        maxlen = int(self.cache_cfg.get("maxlen", kwargs.get("maxlen", 200_000)))

        # Back-compat: accept old key per_expiry_maxlen, but semantics are term-bucketed.
        per_term_maxlen_any = self.cache_cfg.get("per_term_maxlen")
        if per_term_maxlen_any is None:
            per_term_maxlen_any = self.cache_cfg.get("per_expiry_maxlen", kwargs.get("per_term_maxlen", kwargs.get("per_expiry_maxlen", 50_000)))
        per_term_maxlen = int(per_term_maxlen_any)

        term_bucket_ms_any = self.cache_cfg.get("term_bucket_ms", kwargs.get("term_bucket_ms", 86_400_000))
        term_bucket_ms = int(term_bucket_ms_any)

        if maxlen <= 0:
            raise ValueError("option_trades cache.maxlen must be > 0")
        if per_term_maxlen <= 0:
            raise ValueError("option_trades cache.per_term_maxlen must be > 0")
        if term_bucket_ms <= 0:
            raise ValueError("option_trades cache.term_bucket_ms must be > 0")

        self.cache = OptionTradesBucketedCache(
            maxlen=maxlen,
            per_term_maxlen=per_term_maxlen,
            term_bucket_ms=term_bucket_ms,
        )

        # dataframe view columns only (storage keeps full schema via to_dict/aux)
        self.columns = kwargs.get(
            "columns",
            [
                "data_ts",
                "instrument_name",
                "expiry_ts",
                "expiry_ymd",
                "direction",
                "price",
                "amount",
                "contracts",
                "iv",
                "index_price",
                "mark_price",
                "tick_direction",
                "trade_seq",
                "trade_id",
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
            raise TypeError("option_trades 'gap' must be a dict")
        min_gap_ms = gap_cfg.get("min_gap_ms")
        self.gap_min_gap_ms = int(min_gap_ms) if min_gap_ms is not None else None
        # Backfill worker is wired by runtime/apps; do not initialize here.
        self._backfill_worker = None
        self._backfill_emit = None
        self._anchor_ts = None
        self._logger = get_logger(__name__)
        self._engine_mode = _coerce_engine_mode(kwargs.get("mode"))
        self._data_root = resolve_data_root(
            __file__,
            levels_up=4,
            data_root=kwargs.get("data_root") or kwargs.get("cleaned_root"),
        )

        log_debug(
            self._logger,
            "OptionTradesDataHandler initialized",
            symbol=self.symbol,
            source=self.source,
            maxlen=maxlen,
            per_term_maxlen=per_term_maxlen,
            term_bucket_ms=term_bucket_ms,
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
        log_debug(self._logger, "OptionTradesDataHandler align_to", symbol=self.symbol, anchor_ts=self._anchor_ts)

    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        # IO-free by default. Bootstrap only reads local storage.
        log_debug(
            self._logger,
            "OptionTradesDataHandler.bootstrap (no-op)",
            symbol=self.symbol,
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
            "OptionTradesDataHandler.load_history (no-op)",
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
        """Ingest one or many option trade payloads.

        Accepted payloads:
          - Mapping[str, Any] (single trade) in tick.payload
          - Sequence[Mapping[str, Any]] (batch) in tick.payload
          - pandas.DataFrame (batch) in tick.payload

        Required keys per trade (Deribit-like):
          - timestamp (epoch ms)
          - instrument_name
          - price, amount, direction
          - trade_id, trade_seq, tick_direction
          - iv, index_price, mark_price, contracts
        """
        payload = tick.payload
        if isinstance(payload, Mapping):
            payload = dict(payload)
            payload.setdefault("data_ts", int(tick.data_ts))
        rows = _coerce_to_rows(payload)
        if not rows:
            return

        # Drop malformed rows early (required fields).
        rows = [r for r in rows if r.get("instrument_name") is not None]
        if not rows:
            return

        # Deterministic ordering inside batch.
        # Deribit has multiple trades with same timestamp; trade_seq is a good tie-breaker.
        def _key(r: Mapping[str, Any]) -> tuple[int, int, str]:
            ts_any = r.get("data_ts")
            if ts_any is None:
                return (0, 0, "")
            ts = int(ts_any)

            seq_any = r.get("trade_seq")
            seq = int(seq_any) if seq_any is not None else 0

            tid_any = r.get("trade_id")
            tid = "" if tid_any is None else str(tid_any)
            return (ts, seq, tid)

        rows.sort(key=_key)

        pushed = 0
        last = self.cache.last()
        last_ts = int(last.data_ts) if last is not None else None
        for r in rows:
            try:
                market = _resolve_market(
                    self.market,
                    r,
                    last_ts=last_ts,
                    data_ts=int(ts) if (ts := r.get("data_ts")) is not None else None,
                    min_gap_ms=self.gap_min_gap_ms,
                )
                e = OptionTradeEvent.from_deribit(trade=r, symbol=self.symbol, market=market)
            except Exception as ex:
                log_debug(self._logger, "OptionTradesDataHandler.on_new_tick: bad trade skipped", err=str(ex))
                continue
            self.cache.push(e)
            last_ts = int(e.data_ts)
            pushed += 1

        if pushed:
            log_debug(self._logger, "OptionTradesDataHandler.on_new_tick", symbol=self.symbol, pushed=pushed)

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

    def get_snapshot(self, ts: int | None = None) -> OptionTradeEvent | None:
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
        evs = list(self.cache.get_n_before(t, int(n)))
        return self.to_df(evs, columns=self.columns)

    # ------------------------------------------------------------------
    # Term helpers (optional)
    # term := expiry_ts - data_ts (DTE-like; bucketed by term_bucket_ms)
    # ------------------------------------------------------------------

    def term_buckets(self) -> list[int]:
        """Return available term bucket keys (ms).

        term_key_ms = floor((expiry_ts - data_ts) / term_bucket_ms) * term_bucket_ms
        """
        return self.cache.term_buckets()

    def get_snapshot_for_term(self, *, term_key_ms: int, ts: int | None = None) -> OptionTradeEvent | None:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        return self.cache.get_at_or_before_for_term(int(term_key_ms), t)

    def window_for_term(self, *, term_key_ms: int, ts: int | None = None, n: int = 1) -> pd.DataFrame:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return pd.DataFrame()
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        evs = list(self.cache.get_n_before_for_term(int(term_key_ms), t, int(n)))
        return self.to_df(evs, columns=self.columns)

    # ------------------------------------------------------------------
    # Expiry helpers (optional)
    # ------------------------------------------------------------------

    def expiries(self) -> list[int]:
        """Return available expiry timestamps (epoch ms) present in the cache."""
        # cache.py exposes expiry_list() for the real expiry index
        if hasattr(self.cache, "expiry_list"):
            return list(self.cache.expiry_list())  # type: ignore[attr-defined]
        # fallback for older cache implementations
        return list(getattr(self.cache, "by_expiry", {}).keys())

    def get_snapshot_for_expiry(self, *, expiry_ts: int, ts: int | None = None) -> OptionTradeEvent | None:
        """Latest trade for a specific expiry with event.data_ts <= ts."""
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        return self.cache.get_at_or_before_for_expiry(int(expiry_ts), t)

    def window_for_expiry(self, *, expiry_ts: int, ts: int | None = None, n: int = 1) -> pd.DataFrame:
        """Last n trades for a specific expiry with event.data_ts <= ts."""
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return pd.DataFrame()
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        evs = list(self.cache.get_n_before_for_expiry(int(expiry_ts), t, int(n)))
        return self.to_df(evs, columns=self.columns)

    # ------------------------------------------------------------------
    # Conversions
    # ------------------------------------------------------------------

    @staticmethod
    def to_df(events: Iterable[OptionTradeEvent], *, columns: list[str] | None) -> pd.DataFrame:
        """Convert events to a DataFrame view.

        Rules:
          - If columns is None: full to_dict() per row
          - Else: select from core dict first, then from aux
        """
        rows: list[dict[str, Any]] = []
        for e in events:
            d = dict(e.to_dict())
            aux = d.get("aux") or {}

            if columns is None:
                # keep aux nested as dict
                rows.append(d)
                continue

            out: dict[str, Any] = {}
            for c in columns:
                if c in d:
                    out[c] = d[c]
                elif isinstance(aux, dict) and c in aux:
                    out[c] = aux.get(c)
                else:
                    out[c] = None
            rows.append(out)

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Legacy / misc
    # ------------------------------------------------------------------

    def reset(self) -> None:
        log_info(self._logger, "OptionTradesDataHandler reset requested", symbol=self.symbol)
        self.cache.clear()

    def _maybe_backfill(self, *, target_ts: int) -> None:
        if not self._should_backfill():
            return
        self._backfill_to_target(target_ts=int(target_ts))

    # ------------------------------------------------------------------
    # Backfill helpers (realtime/mock only)
    # ------------------------------------------------------------------

    def _should_backfill(self) -> bool:
        return self._engine_mode in (EngineMode.REALTIME, EngineMode.MOCK)

    def _bootstrap_from_files(self, *, anchor_ts: int, lookback: Any | None) -> None:
        bars = _coerce_lookback_bars(lookback, self.interval_ms, getattr(self.cache, "maxlen", None))
        if bars is None or bars <= 0 or self.interval_ms is None:
            return
        start_ts = int(anchor_ts) - (int(bars) - 1) * int(self.interval_ms)
        end_ts = int(anchor_ts)
        log_info(
            self._logger,
            "option_trades.bootstrap.start",
            symbol=self.symbol,
            asset=self.asset,
            venue=self.source,
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
                "option_trades.bootstrap.done",
                symbol=self.symbol,
                asset=self.asset,
                venue=self.source,
                loaded_count=int(loaded),
                cache_size=len(getattr(self.cache, "buffer", [])),
            )
        except Exception as exc:
            log_exception(
                self._logger,
                "option_trades.bootstrap.error",
                symbol=self.symbol,
                asset=self.asset,
                venue=self.source,
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
                log_warn(
                    self._logger,
                    "option_trades.backfill.no_lookback",
                    symbol=self.symbol,
                    asset=self.asset,
                    venue=self.source,
                    target_ts=int(target_ts),
                )
                return
            start_ts = int(target_ts) - (int(bars) - 1) * int(self.interval_ms)
            end_ts = int(target_ts)
            log_warn(
                self._logger,
                "option_trades.backfill.cold_start",
                symbol=self.symbol,
                asset=self.asset,
                venue=self.source,
                target_ts=int(target_ts),
                interval_ms=int(self.interval_ms),
                start_ts=start_ts,
                end_ts=end_ts,
            )
            try:
                loaded = self._backfill_from_source(start_ts=start_ts, end_ts=end_ts, target_ts=target_ts)
                log_info(
                    self._logger,
                    "option_trades.backfill.done",
                    symbol=self.symbol,
                    asset=self.asset,
                    venue=self.source,
                    loaded_count=int(loaded),
                    cache_size=len(getattr(self.cache, "buffer", [])),
                )
            except Exception as exc:
                log_exception(
                    self._logger,
                    "option_trades.backfill.error",
                    symbol=self.symbol,
                    asset=self.asset,
                    venue=self.source,
                    err=str(exc),
                )
            return
        gap_threshold = int(target_ts) - int(self.interval_ms)
        if int(last_ts) >= gap_threshold:
            return
        start_ts = int(last_ts) + int(self.interval_ms)
        end_ts = int(target_ts)
        log_warn(
            self._logger,
            "option_trades.gap_detected",
            symbol=self.symbol,
            asset=self.asset,
            venue=self.source,
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
                "option_trades.backfill.done",
                symbol=self.symbol,
                asset=self.asset,
                venue=self.source,
                loaded_count=int(loaded),
                cache_size=len(getattr(self.cache, "buffer", [])),
            )
            post_last = self.last_timestamp()
            if post_last is None or int(post_last) < (int(target_ts) - int(self.interval_ms)):
                log_warn(
                    self._logger,
                    "option_trades.backfill.incomplete",
                    symbol=self.symbol,
                    asset=self.asset,
                    venue=self.source,
                    last_ts=int(post_last) if post_last is not None else None,
                    target_ts=int(target_ts),
                    interval_ms=int(self.interval_ms),
                )
        except Exception as exc:
            log_exception(
                self._logger,
                "option_trades.backfill.error",
                symbol=self.symbol,
                asset=self.asset,
                venue=self.source,
                err=str(exc),
            )

    def _load_from_files(self, *, start_ts: int, end_ts: int) -> int:
        paths = resolve_cleaned_paths(
            data_root=self._data_root,
            domain="option_trades",
            venue=self.source,
            asset=self.asset,
            start_ts=int(start_ts),
            end_ts=int(end_ts),
        )
        if not paths:
            return 0
        last_ts = self.last_timestamp()
        count = 0
        for row in _iter_parquet_rows(paths, start_ts=int(start_ts), end_ts=int(end_ts)):
            ts = _infer_data_ts(row)
            if last_ts is not None and int(ts) <= int(last_ts):
                continue
            self.on_new_tick(_tick_from_payload(row, symbol=self.symbol))
            last_ts = int(ts)
            count += 1
        return count

    def _backfill_from_source(self, *, start_ts: int, end_ts: int, target_ts: int) -> int:
        worker = self._backfill_worker
        if worker is None:
            log_warn(
                self._logger,
                "option_trades.backfill.no_worker",
                symbol=self.symbol,
                asset=self.asset,
                venue=self.source,
                start_ts=int(start_ts),
                end_ts=int(end_ts),
            )
            return 0
        backfill = getattr(worker, "backfill", None)
        if not callable(backfill):
            log_warn(
                self._logger,
                "option_trades.backfill.no_worker_method",
                symbol=self.symbol,
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


def _coerce_to_rows(x: Any) -> list[dict[str, Any]]:
    def _stringify_keys(m: Mapping[Any, Any]) -> dict[str, Any]:
        return {str(k): v for k, v in m.items()}

    if x is None:
        return []

    if isinstance(x, pd.DataFrame):
        df = x
        if df.empty:
            return []
        recs = df.to_dict(orient="records")
        return [{str(k): v for k, v in r.items()} for r in recs]

    if isinstance(x, Mapping):
        return [_stringify_keys(x)]

    if isinstance(x, Sequence) and not isinstance(x, (str, bytes, bytearray)):
        out: list[dict[str, Any]] = []
        for r in x:
            if isinstance(r, Mapping):
                out.append(_stringify_keys(r))
        return out

    # last resort: try DataFrame
    try:
        df = pd.DataFrame(x)
    except Exception:
        return []
    if df.empty:
        return []
    recs = df.to_dict(orient="records")
    return [{str(k): v for k, v in r.items()} for r in recs]


def _tick_from_payload(payload: Mapping[str, Any], *, symbol: str) -> IngestionTick:
    data_ts = _infer_data_ts(payload)
    return IngestionTick(
        timestamp=int(data_ts),
        data_ts=int(data_ts),
        domain="option_trades",
        symbol=symbol,
        payload=payload,
    )


def _infer_data_ts(payload: Mapping[str, Any]) -> int:
    if "data_ts" in payload:
        return _coerce_epoch_ms(payload.get("data_ts"))
    if "timestamp" in payload:
        return _coerce_epoch_ms(payload.get("timestamp"))
    raise ValueError("Option trade payload missing data_ts/timestamp for backfill")


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
        bars = lookback.get("bars")
        if bars is not None and interval_ms is not None:
            return int(float(bars) * int(interval_ms))
        return None
    if isinstance(lookback, (int, float)):
        if interval_ms is not None:
            return int(float(lookback) * int(interval_ms))
        return int(float(lookback))
    if isinstance(lookback, str):
        ms = to_interval_ms(lookback)
        return int(ms) if ms is not None else None
    return None


def _coerce_lookback_bars(lookback: Any, interval_ms: int | None, max_bars: int | None) -> int | None:
    if interval_ms is None or interval_ms <= 0:
        return None
    window_ms = _coerce_lookback_ms(lookback, interval_ms)
    if window_ms is None:
        return None
    bars = max(1, int(math.ceil(int(window_ms) / int(interval_ms))))
    if max_bars is not None:
        bars = min(bars, int(max_bars))
    return bars


def _coerce_engine_mode(mode: Any) -> EngineMode | None:
    if isinstance(mode, EngineMode):
        return mode
    if isinstance(mode, str):
        try:
            return EngineMode(mode)
        except Exception:
            return None
    return None


def _iter_parquet_rows(paths: Iterable[Path], *, start_ts: int, end_ts: int) -> Iterable[dict[str, Any]]:
    for fp in paths:
        if not fp.exists():
            continue
        df = pd.read_parquet(fp)
        if df is None or df.empty:
            continue
        if "data_ts" not in df.columns:
            if "timestamp" in df.columns:
                df["data_ts"] = df["timestamp"]
            else:
                continue
        df["data_ts"] = df["data_ts"].map(_coerce_epoch_ms)
        if start_ts is not None:
            df = df[df["data_ts"] >= int(start_ts)]
        if end_ts is not None:
            df = df[df["data_ts"] <= int(end_ts)]
        if df.empty:
            continue
        df = df.sort_values("data_ts", kind="mergesort")
        for rec in df.to_dict(orient="records"):
            out = {str(k): v for k, v in rec.items()}
            yield out
