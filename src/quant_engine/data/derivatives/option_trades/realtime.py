from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler
from quant_engine.utils.logger import get_logger, log_debug, log_info

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

    Config mapping (Strategy.DATA.*.option_trades):
      - source: origin identifier (e.g., "DERIBIT") (kept for metadata/routing)
      - cache.maxlen: global cache maxlen
      - cache.per_term_maxlen: per-term bucket maxlen (term := expiry_ts - data_ts)
      - cache.term_bucket_ms: term bucket width in ms (default: 1 day)
      - columns: dataframe view columns (core or aux keys)
    """

    symbol: str
    source: str
    columns: list[str] | None

    cache_cfg: dict[str, Any]
    cache: OptionTradesBucketedCache

    _anchor_ts: int | None
    _logger: Any

    def __init__(self, symbol: str, **kwargs: Any):
        self.symbol = symbol
        self.source = str(kwargs.get("source") or "DERIBIT")

        cache = kwargs.get("cache") or {}
        if not isinstance(cache, dict):
            raise TypeError("option_trades 'cache' must be a dict")
        self.cache_cfg = dict(cache)

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

        self._anchor_ts = None
        self._logger = get_logger(__name__)

        log_debug(
            self._logger,
            "OptionTradesDataHandler initialized",
            symbol=self.symbol,
            source=self.source,
            maxlen=maxlen,
            per_term_maxlen=per_term_maxlen,
            term_bucket_ms=term_bucket_ms,
        )

    # ------------------------------------------------------------------
    # Lifecycle (realtime/mock)
    # ------------------------------------------------------------------

    def align_to(self, ts: int) -> None:
        self._anchor_ts = int(ts)
        log_debug(self._logger, "OptionTradesDataHandler align_to", symbol=self.symbol, anchor_ts=self._anchor_ts)

    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        # IO-free by default
        log_debug(
            self._logger,
            "OptionTradesDataHandler.bootstrap (no-op)",
            symbol=self.symbol,
            anchor_ts=anchor_ts,
            lookback=lookback,
        )

    # ------------------------------------------------------------------
    # Streaming tick API
    # ------------------------------------------------------------------

    def on_new_tick(self, payload: Any) -> None:
        """Ingest one or many option trade payloads.

        Accepted payloads:
          - Mapping[str, Any] (single trade)
          - Sequence[Mapping[str, Any]] (batch)
          - pandas.DataFrame (batch)

        Required keys per trade (Deribit-like):
          - timestamp (epoch ms)
          - instrument_name
          - price, amount, direction
          - trade_id, trade_seq, tick_direction
          - iv, index_price, mark_price, contracts
        """
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
        for r in rows:
            try:
                e = OptionTradeEvent.from_deribit(trade=r, symbol=self.symbol)
            except Exception as ex:
                log_debug(self._logger, "OptionTradesDataHandler.on_new_tick: bad trade skipped", err=str(ex))
                continue
            self.cache.push(e)
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
