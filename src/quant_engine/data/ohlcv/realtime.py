from __future__ import annotations

from typing import Any

import pandas as pd
import numpy as np
from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler, to_interval_ms
from quant_engine.utils.logger import get_logger, log_debug, log_info

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


    # ------------------------------------------------------------------
    # Streaming tick API
    # ------------------------------------------------------------------

    def on_new_tick(self, bar: Any) -> None:
        """
        Ingest a single OHLCV payload (event-time fact).

        Payload contract:
          - Represents data that has already occurred (event-time).
          - Must be domain-typed (OHLCV semantics), not raw exchange messages.
          - Must contain a resolvable event-time:
              * 'timestamp' (epoch ms int), OR
              * 'open_time' (ms epoch or datetime).
          - May be a single bar (dict/Series) or multiple bars (DataFrame).
          - Ingest is append-only and unconditional.
        """
        # Payload boundary: from this point on, data is treated as an immutable event-time fact.
        df = _coerce_ohlcv_to_df(bar)
        if df is None or df.empty:
            log_debug(
                self._logger,
                "RealTimeDataHandler.on_new_tick: empty bar ignored",
                symbol=self.symbol,
            )
            return

        df = _ensure_timestamp(df)

        if "timestamp" in df.columns:
            df = df[df["timestamp"].notna()].copy()

        # Ensure deterministic ingestion order by event-time (Source may batch or reorder).
        df = df.sort_values("timestamp")
        for _, row in df.iterrows():
            assert self._anchor_ts is not None
            row = row.to_dict()
            snap = OHLCVSnapshot.from_bar_aligned(
                timestamp = row["timestamp"],
                bar=row,
                symbol=self.symbol,
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
    if "timestamp" not in df.columns and "ts" in df.columns:
        df = df.rename(columns={"ts": "timestamp"})

    return df


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

    if "timestamp" in out.columns:
        out["timestamp"] = _to_epoch_ms_series(out["timestamp"])
        out = out[out["timestamp"].notna()].copy()
        out["timestamp"] = out["timestamp"].astype("int64")
        return out

    if "close_time" not in out.columns:
        raise KeyError("OHLCV bar must contain 'timestamp' or 'close_time'")

    s = out["close_time"]
    if pd.api.types.is_datetime64_any_dtype(s):
        dt = pd.to_datetime(s, utc=True, errors="coerce")
        ns = dt.astype("int64")
        ms = (ns // 1_000_000).astype("Int64")
        out["timestamp"] = ms
        out = out[out["timestamp"].notna()].copy()
        out["timestamp"] = out["timestamp"].astype("int64")
        return out

    # assume numeric seconds-or-ms epoch
    out["timestamp"] = _to_epoch_ms_series(s)
    out = out[out["timestamp"].notna()].copy()
    out["timestamp"] = out["timestamp"].astype("int64")
    return out