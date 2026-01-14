from __future__ import annotations

import math
from typing import Any, Mapping, cast

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
from quant_engine.utils.cleaned_path_resolver import (
    base_asset_from_symbol,
    resolve_cleaned_paths,
    resolve_domain_symbol_keys,
    symbol_from_base_asset,
)
from quant_engine.utils.paths import resolve_data_root
from ingestion.trades.source import TradesFileSource
from ingestion.contracts.tick import IngestionTick, _coerce_epoch_ms

from .cache import TradesDataCache
from .snapshot import TradesSnapshot


class TradesDataHandler(RealTimeDataHandler):
    """
    Runtime Trades handler (mode-agnostic).

    Snapshot-native handler for trade prints / aggregated trades.
    Responsibilities:
      - Normalize incoming trade payloads
      - Construct TradesSnapshot objects
      - Push snapshots into TradesDataCache
      - Provide timestamp-aligned snapshot / DataFrame views

    Non-responsibilities:
      - No aggregation (VWAP, bars, imbalance)
      - No IO (network / filesystem)
      - No polling or cadence control (ingestion poll_time is external)
    """

    symbol: str
    interval: str | None
    interval_ms: int | None
    cache: TradesDataCache
    columns: list[str] | None
    market: MarketSpec
    gap_min_gap_ms: int | None
    _backfill_worker: Any | None
    _backfill_emit: Any | None
    bootstrap_cfg: dict[str, Any] | None

    _anchor_ts: int | None
    _logger: Any
    _engine_mode: EngineMode | None
    _data_root: Any

    def __init__(self, symbol: str, **kwargs: Any):
        self.symbol = symbol

        interval = kwargs.get("interval")
        if interval is not None and (not isinstance(interval, str) or not interval):
            raise ValueError("Trades 'interval' must be a non-empty string if provided")
        self.interval = interval
        interval_ms = to_interval_ms(self.interval) if self.interval is not None else None
        self.interval_ms = int(interval_ms) if interval_ms is not None else None

        cache_cfg = kwargs.get("cache") or {}
        if not isinstance(cache_cfg, dict):
            raise TypeError("Trades 'cache' must be a dict")
        self.bootstrap_cfg = kwargs.get("bootstrap") or None
        
        maxlen = int(cache_cfg.get("maxlen", kwargs.get("maxlen", 10_000)))
        if maxlen <= 0:
            raise ValueError("Trades cache.maxlen must be > 0")

        self.cache = TradesDataCache(maxlen=maxlen)
        self.market = ensure_market_spec(
            kwargs.get("market"),
            default_venue=str(kwargs.get("venue", kwargs.get("source", "unknown"))),
            default_asset_class=str(kwargs.get("asset_class", "crypto")),
            default_timezone=str(kwargs.get("timezone", "UTC")),
            default_calendar=str(kwargs.get("calendar", "24x7")),
            default_session=str(kwargs.get("session", "24x7")),
            default_currency=kwargs.get("currency"),
        )
        base = base_asset_from_symbol(self.symbol)
        self.display_symbol, self._symbol_aliases = resolve_domain_symbol_keys(
            "trades",
            self.symbol,
            base,
            getattr(self.market, "currency", None),
        )
        gap_cfg = kwargs.get("gap") or {}
        if not isinstance(gap_cfg, dict):
            raise TypeError("Trades 'gap' must be a dict")
        min_gap_ms = gap_cfg.get("min_gap_ms")
        self.gap_min_gap_ms = int(min_gap_ms) if min_gap_ms is not None else None
        # Backfill worker is wired by runtime/apps; do not initialize here.
        self._backfill_worker = None
        self._backfill_emit = None

        # DataFrame view columns (legacy / feature convenience)
        self.columns = kwargs.get(
            "columns",
            ["data_ts", "price", "size", "side"],
        )

        self._anchor_ts = None
        self._logger = get_logger(__name__)
        self._engine_mode = _coerce_engine_mode(kwargs.get("mode"))
        self._data_root = resolve_data_root(
            __file__,
            levels_up=4,
            data_root=kwargs.get("data_root") or kwargs.get("cleaned_root"),
        )
        self.source_id = _resolve_source_id(
            source_id=kwargs.get("source_id"),
            mode=self._engine_mode,
            data_root=self._data_root,
            source=kwargs.get("source") or kwargs.get("venue"),
        )

        log_debug(
            self._logger,
            "TradesDataHandler initialized",
            symbol=self.display_symbol, instrument_symbol=self.symbol,
            maxlen=maxlen,
        )

    def set_external_source(self, worker: Any | None, *, emit: Any | None = None) -> None:
        """Attach ingestion worker for backfill (wired by runtime/apps)."""
        self._backfill_worker = worker
        self._backfill_emit = emit

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def align_to(self, ts: int) -> None:
        """Clamp implicit reads to ts (anti-lookahead anchor).

        `ts` is the runtime observation timestamp, not ingestion poll_time.
        """
        self._anchor_ts = int(ts)
        log_debug(
            self._logger,
            "TradesDataHandler align_to",
            symbol=self.display_symbol, instrument_symbol=self.symbol,
            anchor_ts=self._anchor_ts,
        )

    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        """No-op bootstrap (IO-free). Bootstrap only reads local storage."""
        log_debug(
            self._logger,
            "TradesDataHandler.bootstrap (no-op)",
            symbol=self.display_symbol, instrument_symbol=self.symbol,
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
            "TradesDataHandler.load_history (no-op)",
            symbol=self.display_symbol, instrument_symbol=self.symbol,
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
        Ingest trade payload(s).

        Accepted inputs:
          - dict (single trade) in tick.payload
          - list[dict] / iterable in tick.payload
          - DataFrame in tick.payload
        """
        if tick.domain != "trades" or tick.symbol not in self._symbol_aliases:
            return
        expected_source = getattr(self, "source_id", None)
        tick_source = getattr(tick, "source_id", None)
        if expected_source is not None and tick_source != expected_source:
            return
        payload = tick.payload
        if isinstance(payload, Mapping):
            payload = dict(payload)
            payload.setdefault("data_ts", int(tick.data_ts))
        df = _coerce_trades_to_df(payload)
        if df is None or df.empty:
            return

        if "data_ts" not in df.columns:
            raise KeyError("Trade payload must contain 'data_ts'")

        df = df.sort_values("data_ts")

        for _, row in df.iterrows():
            if self._anchor_ts is None:
                raise RuntimeError("TradesDataHandler.on_new_tick called before align_to()")
            payload = row.to_dict()
            last = self.cache.last()
            last_ts = int(last.data_ts) if last is not None else None
            ts = payload.get("data_ts")
            assert ts is not None, "Trade payload must contain event-time 'data_ts'"
            market = _resolve_market(
                self.market,
                payload,
                last_ts=last_ts,
                data_ts=int(ts),
                min_gap_ms=self.gap_min_gap_ms,
            )
            snap = TradesSnapshot.from_trade_aligned(
                timestamp=int(ts),
                trade=payload,
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

    def get_snapshot(self, ts: int | None = None) -> TradesSnapshot | None:
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
        log_info(self._logger, "TradesDataHandler reset requested", symbol=self.display_symbol, instrument_symbol=self.symbol)
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
            "trades.bootstrap.start",
            symbol=self.display_symbol, instrument_symbol=self.symbol,
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
                "trades.bootstrap.done",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                loaded_count=int(loaded),
                cache_size=len(getattr(self.cache, "buffer", [])),
            )
        except Exception as exc:
            log_warn(
                self._logger,
                "trades.bootstrap.error",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
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
                log_warn(
                    self._logger,
                    "trades.backfill.no_lookback",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    target_ts=int(target_ts),
                )
                return
            start_ts = int(target_ts) - (int(bars) - 1) * int(self.interval_ms)
            end_ts = int(target_ts)
            log_warn(
                self._logger,
                "trades.backfill.cold_start",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                target_ts=int(target_ts),
                interval_ms=int(self.interval_ms),
                start_ts=start_ts,
                end_ts=end_ts,
            )
            try:
                loaded = self._backfill_from_source(start_ts=start_ts, end_ts=end_ts, target_ts=target_ts)
                log_info(
                    self._logger,
                    "trades.backfill.done",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    loaded_count=int(loaded),
                    cache_size=len(getattr(self.cache, "buffer", [])),
                )
            except Exception as exc:
                log_exception(
                    self._logger,
                    "trades.backfill.error",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
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
            "trades.gap_detected",
            symbol=self.display_symbol, instrument_symbol=self.symbol,
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
                "trades.backfill.done",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                loaded_count=int(loaded),
                cache_size=len(getattr(self.cache, "buffer", [])),
            )
            post_last = self.last_timestamp()
            if post_last is None or int(post_last) < (int(target_ts) - int(self.interval_ms)):
                log_warn(
                    self._logger,
                    "trades.backfill.incomplete",
                    symbol=self.display_symbol, instrument_symbol=self.symbol,
                    last_ts=int(post_last) if post_last is not None else None,
                    target_ts=int(target_ts),
                    interval_ms=int(self.interval_ms),
                )
        except Exception as exc:
            log_exception(
                self._logger,
                "trades.backfill.error",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                err=str(exc),
            )

    def _load_from_files(self, *, start_ts: int, end_ts: int) -> int:
        symbol_for_paths = symbol_from_base_asset(self.symbol)
        paths = resolve_cleaned_paths(
            data_root=self._data_root,
            domain="trades",
            symbol=symbol_for_paths,
            start_ts=int(start_ts),
            end_ts=int(end_ts),
        )
        if not paths:
            return 0
        source = TradesFileSource(
            root="cleaned/trades",
            symbol=symbol_for_paths,
            start_ms=int(start_ts),
            end_ms=int(end_ts),
            paths=paths,
        )
        last_ts = self.last_timestamp()
        count = 0
        for row in source:
            ts = _infer_data_ts(row)
            if last_ts is not None and int(ts) <= int(last_ts):
                continue
            self.on_new_tick(_tick_from_payload(row, symbol=self.display_symbol, source_id=getattr(self, "source_id", None)))
            last_ts = int(ts)
            count += 1
        return count

    def _backfill_from_source(self, *, start_ts: int, end_ts: int, target_ts: int) -> int:
        worker = self._backfill_worker
        if worker is None:
            log_info(
                self._logger,
                "trades.backfill.no_worker",
                symbol=self.display_symbol, instrument_symbol=self.symbol,
                start_ts=int(start_ts),
                end_ts=int(end_ts),
            )
            return 0
        backfill = getattr(worker, "backfill", None)
        if not callable(backfill):
            log_info(
                self._logger,
                "trades.backfill.no_worker_method",
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


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _coerce_trades_to_df(x: Any) -> pd.DataFrame | None:
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


def _tick_from_payload(payload: Mapping[str, Any], *, symbol: str, source_id: str | None = None) -> IngestionTick:
    data_ts = _infer_data_ts(payload)
    return IngestionTick(
        timestamp=int(data_ts),
        data_ts=int(data_ts),
        domain="trades",
        symbol=symbol,
        payload=payload,
        source_id=source_id,
    )


def _infer_data_ts(payload: Mapping[str, Any]) -> int:
    if "data_ts" in payload:
        return _coerce_epoch_ms(payload.get("data_ts"))
    if "timestamp" in payload:
        return _coerce_epoch_ms(payload.get("timestamp"))
    raise ValueError("Trade payload missing data_ts/timestamp for backfill")


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
