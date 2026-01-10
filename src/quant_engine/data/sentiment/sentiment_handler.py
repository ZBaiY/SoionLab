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
from quant_engine.utils.cleaned_path_resolver import resolve_cleaned_paths
from quant_engine.utils.paths import resolve_data_root
from ingestion.sentiment.source import SentimentFileSource
from ingestion.contracts.tick import IngestionTick, _coerce_epoch_ms

from .cache import SentimentDataCache
from .snapshot import SentimentSnapshot


class SentimentDataHandler(RealTimeDataHandler):
    """
    Runtime Sentiment handler (mode-agnostic).
    """

    symbol: str
    provider: str | None
    interval: str | None
    interval_ms: int | None
    cache: SentimentDataCache
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
        self.provider = None

        interval = kwargs.get("interval")
        if interval is not None and (not isinstance(interval, str) or not interval):
            raise ValueError("Sentiment 'interval' must be a non-empty string if provided")
        self.interval = interval
        interval_ms = to_interval_ms(self.interval) if self.interval is not None else None
        self.interval_ms = int(interval_ms) if interval_ms is not None else None

        provider_any = kwargs.get("provider") or kwargs.get("source") or kwargs.get("venue")
        if provider_any is not None:
            self.provider = str(provider_any)

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
        # Backfill worker is wired by runtime/apps; do not initialize here.
        self._backfill_worker = None
        self._backfill_emit = None

        # DataFrame view columns (legacy / research convenience)
        self.columns = kwargs.get(
            "columns",
            ["data_ts", "score", "source"],
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
            source=self.provider or kwargs.get("venue") or kwargs.get("source"),
        )

        log_debug(
            self._logger,
            "SentimentDataHandler initialized",
            symbol=self.symbol,
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
        """Clamp implicit reads to ts (anti-lookahead anchor)."""
        self._anchor_ts = int(ts)
        log_debug(
            self._logger,
            "SentimentDataHandler align_to",
            symbol=self.symbol,
            anchor_ts=self._anchor_ts,
        )

    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        """No-op bootstrap (IO-free). Bootstrap only reads local storage."""
        log_debug(
            self._logger,
            "SentimentDataHandler.bootstrap (no-op)",
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
            "SentimentDataHandler.load_history (no-op)",
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
        Ingest sentiment payload(s).

        Accepted inputs:
          - dict (single event) in tick.payload
          - list[dict] / iterable in tick.payload
          - DataFrame in tick.payload
        """
        if tick.domain != "sentiment" or tick.symbol != self.symbol:
            return
        expected_source = getattr(self, "source_id", None)
        tick_source = getattr(tick, "source_id", None)
        if expected_source is not None and tick_source != expected_source:
            return
        payload = tick.payload
        if isinstance(payload, Mapping):
            payload = dict(payload)
            payload.setdefault("data_ts", int(tick.data_ts))
        df = _coerce_sentiment_to_df(payload)
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

    def _maybe_backfill(self, *, target_ts: int) -> None:
        if not self._should_backfill():
            return
        self._backfill_to_target(target_ts=int(target_ts))

    # ------------------------------------------------------------------
    # Backfill helpers (realtime/mock only)
    # ------------------------------------------------------------------

    def _should_backfill(self) -> bool:
        return self._engine_mode in (EngineMode.REALTIME, EngineMode.MOCK)

    def _provider_for_paths(self) -> str:
        if self.provider:
            return self.provider
        venue = getattr(self.market, "venue", None)
        if venue and str(venue).lower() != "unknown":
            return str(venue)
        return self.symbol

    def _bootstrap_from_files(self, *, anchor_ts: int, lookback: Any | None) -> None:
        bars = _coerce_lookback_bars(lookback, self.interval_ms, getattr(self.cache, "maxlen", None))
        if bars is None or bars <= 0 or self.interval_ms is None:
            return
        start_ts = int(anchor_ts) - (int(bars) - 1) * int(self.interval_ms)
        end_ts = int(anchor_ts)
        provider = self._provider_for_paths()
        log_info(
            self._logger,
            "sentiment.bootstrap.start",
            symbol=self.symbol,
            provider=provider,
            start_ts=start_ts,
            end_ts=end_ts,
            bars=int(bars),
        )
        prev_anchor = self._anchor_ts
        if prev_anchor is None:
            self._anchor_ts = int(anchor_ts)
        try:
            loaded = self._load_from_files(start_ts=start_ts, end_ts=end_ts, provider=provider)
            log_info(
                self._logger,
                "sentiment.bootstrap.done",
                symbol=self.symbol,
                provider=provider,
                loaded_count=int(loaded),
                cache_size=len(getattr(self.cache, "buffer", [])),
            )
        except Exception as exc:
            log_exception(
                self._logger,
                "sentiment.bootstrap.error",
                symbol=self.symbol,
                provider=provider,
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
                    "sentiment.backfill.no_lookback",
                    symbol=self.symbol,
                    target_ts=int(target_ts),
                )
                return
            start_ts = int(target_ts) - (int(bars) - 1) * int(self.interval_ms)
            end_ts = int(target_ts)
            provider = self._provider_for_paths()
            log_warn(
                self._logger,
                "sentiment.backfill.cold_start",
                symbol=self.symbol,
                provider=provider,
                target_ts=int(target_ts),
                interval_ms=int(self.interval_ms),
                start_ts=start_ts,
                end_ts=end_ts,
            )
            try:
                loaded = self._backfill_from_source(start_ts=start_ts, end_ts=end_ts, target_ts=target_ts)
                log_info(
                    self._logger,
                    "sentiment.backfill.done",
                    symbol=self.symbol,
                    provider=provider,
                    loaded_count=int(loaded),
                    cache_size=len(getattr(self.cache, "buffer", [])),
                )
            except Exception as exc:
                log_exception(
                    self._logger,
                    "sentiment.backfill.error",
                    symbol=self.symbol,
                    provider=provider,
                    err=str(exc),
                )
            return
        gap_threshold = int(target_ts) - int(self.interval_ms)
        if int(last_ts) >= gap_threshold:
            return
        start_ts = int(last_ts) + int(self.interval_ms)
        end_ts = int(target_ts)
        provider = self._provider_for_paths()
        log_warn(
            self._logger,
            "sentiment.gap_detected",
            symbol=self.symbol,
            provider=provider,
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
                "sentiment.backfill.done",
                symbol=self.symbol,
                provider=provider,
                loaded_count=int(loaded),
                cache_size=len(getattr(self.cache, "buffer", [])),
            )
            post_last = self.last_timestamp()
            if post_last is None or int(post_last) < (int(target_ts) - int(self.interval_ms)):
                log_warn(
                    self._logger,
                    "sentiment.backfill.incomplete",
                    symbol=self.symbol,
                    provider=provider,
                    last_ts=int(post_last) if post_last is not None else None,
                    target_ts=int(target_ts),
                    interval_ms=int(self.interval_ms),
                )
        except Exception as exc:
            log_exception(
                self._logger,
                "sentiment.backfill.error",
                symbol=self.symbol,
                provider=provider,
                err=str(exc),
            )

    def _load_from_files(self, *, start_ts: int, end_ts: int, provider: str) -> int:
        paths = resolve_cleaned_paths(
            data_root=self._data_root,
            domain="sentiment",
            provider=provider,
            start_ts=int(start_ts),
            end_ts=int(end_ts),
        )
        if not paths:
            return 0
        source = SentimentFileSource(
            root="cleaned/sentiment",
            provider=provider,
            start_ts=int(start_ts),
            end_ts=int(end_ts),
            paths=paths,
        )
        last_ts = self.last_timestamp()
        count = 0
        for row in source:
            ts = _infer_data_ts(row)
            if last_ts is not None and int(ts) <= int(last_ts):
                continue
            self.on_new_tick(_tick_from_payload(row, symbol=self.symbol, source_id=getattr(self, "source_id", None)))
            last_ts = int(ts)
            count += 1
        return count

    def _backfill_from_source(self, *, start_ts: int, end_ts: int, target_ts: int) -> int:
        worker = self._backfill_worker
        if worker is None:
            log_warn(
                self._logger,
                "sentiment.backfill.no_worker",
                symbol=self.symbol,
                start_ts=int(start_ts),
                end_ts=int(end_ts),
            )
            return 0
        backfill = getattr(worker, "backfill", None)
        if not callable(backfill):
            log_warn(
                self._logger,
                "sentiment.backfill.no_worker_method",
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


def _tick_from_payload(payload: Mapping[str, Any], *, symbol: str, source_id: str | None = None) -> IngestionTick:
    data_ts = _infer_data_ts(payload)
    return IngestionTick(
        timestamp=int(data_ts),
        data_ts=int(data_ts),
        domain="sentiment",
        symbol=symbol,
        payload=payload,
        source_id=source_id,
    )


def _infer_data_ts(payload: Mapping[str, Any]) -> int:
    if "data_ts" in payload:
        return _coerce_epoch_ms(payload.get("data_ts"))
    if "timestamp" in payload:
        return _coerce_epoch_ms(payload.get("timestamp"))
    if "ts" in payload:
        return _coerce_epoch_ms(payload.get("ts"))
    raise ValueError("Sentiment payload missing data_ts/timestamp for backfill")


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
