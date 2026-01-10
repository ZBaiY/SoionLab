from __future__ import annotations

import math
from typing import Any, Mapping, cast

import pandas as pd
import numpy as np
from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler, to_interval_ms
from quant_engine.data.contracts.snapshot import (
    MarketSpec,
    ensure_market_spec,
    merge_market_spec,
    classify_gap,
)
from ingestion.contracts.tick import IngestionTick, _coerce_epoch_ms
from quant_engine.utils.logger import get_logger, log_debug, log_info, log_warn, log_exception
from quant_engine.runtime.modes import EngineMode
from quant_engine.utils.cleaned_path_resolver import resolve_cleaned_paths, symbol_from_base_asset
from quant_engine.utils.paths import resolve_data_root
from ingestion.ohlcv.source import OHLCVFileSource

from .cache import OHLCVDataCache
from .snapshot import OHLCVSnapshot



class OHLCVDataHandler(RealTimeDataHandler):
    """Runtime OHLCV handler (mode-agnostic).

    This handler is the *runtime platform* used in ALL modes.

    Config mapping (Strategy.DATA.*.ohlcv):
      - source: data origin identifier (default: "binance"). Runtime handler stores it for routing/logging.
      - interval: bar interval string (e.g., "1m", "15m"). Stored for validation/metadata.
      - bootstrap.lookback: convenience horizon for Engine.bootstrap(); handler may use it as default.
      - cache.max_bars: in-memory cache depth (ring buffer size).

    Important:
      - IO-free by default: no networking, no filesystem.
      - Backtest seeding: `from_historical(...)` then driver replays via `on_new_tick(...)`.
      - Anti-lookahead: reads clamp to `ts` (explicit) or `_anchor_ts` (set by warmup_to).
    """

    # --- declared attributes (protocol/typing) ---
    symbol: str
    interval: str
    interval_ms: int

    columns: list[str] | None
    bootstrap_cfg: dict[str, Any]
    cache_cfg: dict[str, Any]
    cache: OHLCVDataCache
    market: MarketSpec
    gap_min_gap_ms: int | None
    _backfill_worker: Any | None
    _backfill_emit: Any | None
    _engine_mode: EngineMode | None
    _data_root: Any

    _anchor_ts: int | None
    _logger: Any

    def __init__(self, symbol: str, **kwargs: Any):
        """Runtime handler init.

        IMPORTANT: Keep init kwargs-driven.
        Strategy/Loader passes nested handler config via `**cfg`.
        """
        self.symbol = symbol

        # Required semantic fields
        interval = kwargs.get("interval")
        if not isinstance(interval, str) or not interval:
            raise ValueError("OHLCV handler requires non-empty 'interval' (e.g. '1m')")
        self.interval = interval
        interval_ms = to_interval_ms(self.interval)
        if interval_ms is None:
            raise ValueError(f"Invalid interval format: {self.interval}")
        self.interval_ms = int(interval_ms)

        mode = _coerce_engine_mode(kwargs.get("mode"))
        self._engine_mode = mode
        self._data_root = resolve_data_root(
            __file__,
            levels_up=4,
            data_root=kwargs.get("data_root") or kwargs.get("cleaned_root"),
        )
        self.source_id = _resolve_source_id(
            source_id=kwargs.get("source_id"),
            mode=mode,
            data_root=self._data_root,
            source=kwargs.get("source") or kwargs.get("venue"),
        )
        
        # Optional nested configs
        bootstrap = kwargs.get("bootstrap") or {}
        if not isinstance(bootstrap, dict):
            raise TypeError("OHLCV 'bootstrap' must be a dict")
        self.bootstrap_cfg = dict(bootstrap)

        cache = kwargs.get("cache") or {}
        if not isinstance(cache, dict):
            raise TypeError("OHLCV 'cache' must be a dict")
        self.cache_cfg = dict(cache)
        # columns: default VIEW columns only; storage/cache always preserve full schema
        self.columns = kwargs.get("columns", ["open", "high", "low", "close", "volume"])

        # cache depth precedence:
        #   1) cache.max_bars
        #   2) legacy window
        #   3) default
        max_bars = self.cache_cfg.get("maxlen")
        if max_bars is None:
            max_bars = kwargs.get("maxlen", 1000)
        max_bars_i = int(max_bars)
        if max_bars_i <= 0:
            raise ValueError("OHLCV cache.maxlen must be > 0")

        self.cache = OHLCVDataCache(maxlen=max_bars_i)
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
            raise TypeError("OHLCV 'gap' must be a dict")
        min_gap_ms = gap_cfg.get("min_gap_ms")
        self.gap_min_gap_ms = int(min_gap_ms) if min_gap_ms is not None else None
        # Backfill worker is wired by runtime/apps; do not initialize here.
        self._backfill_worker = None
        self._backfill_emit = None

        self._logger = get_logger(__name__)
        self._anchor_ts = None

        log_debug(
            self._logger,
            "RealTimeDataHandler initialized",
            symbol=self.symbol,
            interval=self.interval,
            max_bars=max_bars_i,
            bootstrap=self.bootstrap_cfg,
        )

    def set_external_source(self, worker: Any | None, *, emit: Any | None = None) -> None:
        """Attach ingestion worker for backfill (wired by runtime/apps)."""
        self._backfill_worker = worker
        self._backfill_emit = emit


    # ------------------------------------------------------------------
    # Lifecycle (realtime/mock)
    # ------------------------------------------------------------------

    # align_to(ts) defines the maximum visible event-time for all read APIs.
    def align_to(self, ts: int) -> None:
        """Clamp implicit reads to ts (anti-lookahead anchor)."""
        self._anchor_ts = int(ts)
        log_debug(
            self._logger,
            "RealTimeDataHandler align_to",
            symbol=self.symbol,
            anchor_ts=self._anchor_ts,
        )

    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        """Preload recent data into cache.

        By default: no-op (IO-free handler).

        We still store bootstrap params for observability and to allow later adapters.
        Bootstrap only reads local storage; external backfill is handled separately.
        """
        if lookback is None:
            lookback = self.bootstrap_cfg.get("lookback")

        log_debug(
            self._logger,
            "RealTimeDataHandler.bootstrap (no-op)",
            symbol=self.symbol,
            anchor_ts=anchor_ts,
            interval=self.interval,
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
            "RealTimeDataHandler.load_history (no-op)",
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
        """
        Ingest a single OHLCV tick (event-time fact).

        Payload contract (tick.payload):
          - Represents data that has already occurred (event-time).
          - Must be domain-typed (OHLCV semantics), not raw exchange messages.
          - Must contain a resolvable event-time:
              * 'data_ts' (epoch ms int), OR
              * 'close_time' (ms epoch or datetime).
              * 'open_time' (ms epoch or datetime) + interval_ms.
          - May be a single bar (dict/Series) or multiple bars (DataFrame).
          - Ingest is append-only and unconditional.
        """
        if tick.domain != "ohlcv" or tick.symbol != self.symbol:
            return
        expected_source = getattr(self, "source_id", None)
        tick_source = getattr(tick, "source_id", None)
        if expected_source is not None and tick_source != expected_source:
            return
        # Payload boundary: from this point on, data is treated as an immutable event-time fact.
        payload = dict(tick.payload)
        if "data_ts" not in payload:
            payload["data_ts"] = int(tick.data_ts)
        df = _coerce_ohlcv_to_df(payload)
        if df is None or df.empty:
            log_debug(
                self._logger,
                "RealTimeDataHandler.on_new_tick: empty bar ignored",
                symbol=self.symbol,
            )
            return

        df = _ensure_timestamp(df)

        if "data_ts" in df.columns:
            df = df[df["data_ts"].notna()].copy()

        # Ensure deterministic ingestion order by event-time (Source may batch or reorder).
        df = df.sort_values("data_ts" if "data_ts" in df.columns else "timestamp", kind="mergesort")
        for _, row in df.iterrows():
            assert self._anchor_ts is not None
            ts = row.get("data_ts", row.get("timestamp"))
            assert ts is not None, "OHLCV bar must contain event-time 'data_ts' or 'timestamp'"
            row = row.to_dict()
            last = self.cache.last()
            last_ts = int(last.data_ts) if last is not None else None
            data_ts = int(ts)
            market = _resolve_market(
                self.market,
                row,
                last_ts=last_ts,
                data_ts=data_ts,
                expected_interval_ms=self.interval_ms,
                min_gap_ms=self.gap_min_gap_ms,
            )
            snap = OHLCVSnapshot.from_bar_aligned(
                timestamp = row["data_ts"] if "data_ts" in row else row["timestamp"],
                bar=row,
                symbol=self.symbol,
                market=market,
            )
            self.cache.push(snap)

    # ------------------------------------------------------------------
    # Unified access (timestamp-aligned)
    # ------------------------------------------------------------------

    def last_timestamp(self) -> int | None:
        snap = self.cache.last()
        if snap is None:
            return None

        ts = snap.data_ts
        if self._anchor_ts is not None:
            return min(ts, int(self._anchor_ts))
        return ts

    def get_snapshot(self, ts: int | None = None) -> OHLCVSnapshot | None:
        """Return the latest bar snapshot aligned to ts (anti-lookahead)."""
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None

        # Visibility clamp: ts must not exceed last align_to()
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)

        snap = self.cache.get_at_or_before(int(t))
        if snap is None:
            return None
        return snap

    def window(self, ts: int | None = None, n: int = 1) -> pd.DataFrame:
        """Return a DataFrame of the last n bars aligned to ts (anti-lookahead)."""
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return pd.DataFrame()

        # Visibility clamp: ts must not exceed last align_to()
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)

        snapshots = self.cache.get_n_before(t, n)
        assert self.columns is not None
        rows = [snap.to_dict_col(self.columns) for snap in snapshots]
        df = pd.DataFrame(rows)
        return df

    # ------------------------------------------------------------------
    # Legacy compatibility
    # ------------------------------------------------------------------

    def window_df(self, window: int | None = None) -> pd.DataFrame:
        """Deprecated. Prefer window(ts, n)."""
        return self.window(n=window) if window is not None else self.window()

    def reset(self) -> None:
        log_info(self._logger, "RealTimeDataHandler reset requested", symbol=self.symbol)
        try:
            self.cache.clear()
        except AttributeError:
            pass

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
        if bars is None:
            return
        if bars <= 0:
            return
        start_ts = int(anchor_ts) - (int(bars) - 1) * int(self.interval_ms)
        end_ts = int(anchor_ts)
        log_info(
            self._logger,
            "ohlcv.bootstrap.start",
            symbol=self.symbol,
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
                "ohlcv.bootstrap.done",
                symbol=self.symbol,
                loaded_count=int(loaded),
                cache_size=len(getattr(self.cache, "buffer", [])),
            )
        except Exception as exc:
            log_exception(
                self._logger,
                "ohlcv.bootstrap.error",
                symbol=self.symbol,
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
                    "ohlcv.backfill.no_lookback",
                    symbol=self.symbol,
                    target_ts=int(target_ts),
                )
                return
            start_ts = int(target_ts) - (int(bars) - 1) * int(self.interval_ms)
            end_ts = int(target_ts)
            log_warn(
                self._logger,
                "ohlcv.backfill.cold_start",
                symbol=self.symbol,
                target_ts=int(target_ts),
                interval_ms=int(self.interval_ms),
                start_ts=start_ts,
                end_ts=end_ts,
            )
            try:
                loaded = self._backfill_from_source(start_ts=start_ts, end_ts=end_ts, target_ts=target_ts)
                log_info(
                    self._logger,
                    "ohlcv.backfill.done",
                    symbol=self.symbol,
                    loaded_count=int(loaded),
                    cache_size=len(getattr(self.cache, "buffer", [])),
                )
            except Exception as exc:
                log_exception(
                    self._logger,
                    "ohlcv.backfill.error",
                    symbol=self.symbol,
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
            "ohlcv.gap_detected",
            symbol=self.symbol,
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
                "ohlcv.backfill.done",
                symbol=self.symbol,
                loaded_count=int(loaded),
                cache_size=len(getattr(self.cache, "buffer", [])),
            )
            post_last = self.last_timestamp()
            if post_last is None or int(post_last) < (int(target_ts) - int(self.interval_ms)):
                log_warn(
                    self._logger,
                    "ohlcv.backfill.incomplete",
                    symbol=self.symbol,
                    last_ts=int(post_last) if post_last is not None else None,
                    target_ts=int(target_ts),
                    interval_ms=int(self.interval_ms),
                )
        except Exception as exc:
            log_exception(
                self._logger,
                "ohlcv.backfill.error",
                symbol=self.symbol,
                err=str(exc),
            )

    def _load_from_files(self, *, start_ts: int, end_ts: int) -> int:
        symbol_for_paths = symbol_from_base_asset(self.symbol)
        paths = resolve_cleaned_paths(
            data_root=self._data_root,
            domain="ohlcv",
            symbol=symbol_for_paths,
            interval=self.interval,
            start_ts=int(start_ts),
            end_ts=int(end_ts),
        )
        if not paths:
            return 0
        source = OHLCVFileSource(
            root="cleaned/ohlcv",
            symbol=symbol_for_paths,
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
        for row in source:
            ts = _infer_data_ts(row, interval_ms=self.interval_ms)
            if last_ts is not None and int(ts) <= int(last_ts):
                continue
            self.on_new_tick(_tick_from_payload(
                row,
                symbol=self.symbol,
                interval_ms=self.interval_ms,
                source_id=getattr(self, "source_id", None),
            ))
            last_ts = int(ts)
            count += 1
        return count

    def _backfill_from_source(self, *, start_ts: int, end_ts: int, target_ts: int) -> int:
        worker = self._backfill_worker
        if worker is None:
            log_warn(
                self._logger,
                "ohlcv.backfill.no_worker",
                symbol=self.symbol,
                start_ts=int(start_ts),
                end_ts=int(end_ts),
            )
            return 0
        backfill = getattr(worker, "backfill", None)
        if not callable(backfill):
            log_warn(
                self._logger,
                "ohlcv.backfill.no_worker_method",
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

    


def _coerce_ohlcv_to_df(x: Any) -> pd.DataFrame | None:
    """Coerce common bar payloads into a DataFrame."""
    if x is None:
        return None

    if isinstance(x, pd.DataFrame):
        df = x
    elif isinstance(x, dict):
        df = pd.DataFrame([x])
    else:
        try:
            df = pd.DataFrame(x)
        except Exception:
            return None

    if df.empty:
        return df

    # normalize common column aliases
    if "data_ts" not in df.columns and "ts" in df.columns:
        df = df.rename(columns={"ts": "data_ts"})

    return df


def _tick_from_payload(
    payload: Mapping[str, Any],
    *,
    symbol: str,
    interval_ms: int | None,
    source_id: str | None = None,
) -> IngestionTick:
    data_ts = _infer_data_ts(payload, interval_ms=interval_ms)
    return IngestionTick(
        timestamp=int(data_ts),
        data_ts=int(data_ts),
        domain="ohlcv",
        symbol=symbol,
        payload=payload,
        source_id=source_id,
    )


def _infer_data_ts(payload: Mapping[str, Any], *, interval_ms: int | None) -> int:
    if "data_ts" in payload:
        return _coerce_epoch_ms(payload.get("data_ts"))
    if "close_time" in payload:
        return _coerce_epoch_ms(payload.get("close_time"))
    if "open_time" in payload and interval_ms is not None:
        return _coerce_epoch_ms(payload.get("open_time")) + int(interval_ms)
    raise ValueError("OHLCV payload missing data_ts/close_time/open_time for backfill")


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


def _resolve_source_id(
    *,
    source_id: Any | None,
    mode: EngineMode | None,
    data_root: Any | None,
    source: Any | None,
) -> str | None:
    if source_id is not None:
        return str(source_id)
    if mode == EngineMode.BACKTEST and data_root is not None:
        return str(data_root)
    if source is not None:
        return str(source)
    return None


def _to_epoch_ms_series(s: pd.Series) -> pd.Series:
    """Coerce numeric/strings seconds-or-ms epoch into ms as nullable Int64."""
    x = pd.to_numeric(s, errors="coerce")
    if x.notna().any():
        # Heuristic: seconds are ~1e9, ms are ~1e12
        try:
            mx = float(x.max())
        except Exception:
            mx = 0.0
        if mx < 10_000_000_000:  # treat as seconds
            x = (x * 1000.0).round()
        else:
            x = x.round()
    return x.astype("Int64")


def _ensure_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure df has epoch-ms int 'timestamp' column.

    Accepted inputs:
      - already has 'timestamp' (seconds or ms epoch)
      - has 'open_time' (ms epoch or datetime)
    """
    out = df.copy()

    if "data_ts" in out.columns:
        out["data_ts"] = _to_epoch_ms_series(out["data_ts"])
        out = out[out["data_ts"].notna()].copy()
        out["data_ts"] = out["data_ts"].astype("int64")
        return out

    if "close_time" not in out.columns:
        raise KeyError("OHLCV bar must contain 'data_ts' or 'close_time'")

    s = out["close_time"]
    if pd.api.types.is_datetime64_any_dtype(s):
        dt = pd.to_datetime(s, utc=True, errors="coerce")
        ns = dt.astype("int64")
        ms = (ns // 1_000_000).astype("Int64")
        out["data_ts"] = ms
        out = out[out["data_ts"].notna()].copy()
        out["data_ts"] = out["data_ts"].astype("int64")
        return out

    # assume numeric seconds-or-ms epoch
    out["data_ts"] = _to_epoch_ms_series(s)
    out = out[out["data_ts"].notna()].copy()
    out["data_ts"] = out["data_ts"].astype("int64")
    return out
