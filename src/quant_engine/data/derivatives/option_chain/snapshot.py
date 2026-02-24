from __future__ import annotations

from dataclasses import dataclass, field, replace as _dc_replace
from typing import Any, Callable, Dict

import datetime as dt
import os
import re

import numpy as np
import pandas as pd

from quant_engine.data.contracts.snapshot import Snapshot, MarketInfo, MarketSpec, ensure_market_spec
from quant_engine.utils.num import to_float


def to_ms_int(x: Any) -> int:
    """Coerce a timestamp-like value to epoch milliseconds as int."""
    return int(to_float(x))

def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    val = raw.strip().lower()
    if val in {"1", "true", "yes", "y", "on"}:
        return True
    if val in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)

DROP_AUX_DEFAULT = _env_bool("OPTION_CHAIN_DROP_AUX", default=True)

# --- Deribit helpers (temporary fallback) ---
# Instrument example: BTC-28JUN24-60000-C
_DERIBIT_OPT_RE = re.compile(
    r"^(?P<underlying>[A-Z]+)-(?P<date>\d{2}[A-Z]{3}\d{2})-(?P<strike>[0-9.]+)-(?P<type>[CP])$"
)


def _parse_deribit_expiry_ts_ms(instrument_name: str) -> int:
    """Best-effort Deribit expiry timestamp (epoch ms, UTC).

    Prefer exchange-provided `expiration_timestamp` when available.
    This is only a fallback.
    """
    m = _DERIBIT_OPT_RE.match(instrument_name)
    if not m:
        raise ValueError(f"Unrecognized Deribit instrument_name: {instrument_name}")
    expiry_date = dt.datetime.strptime(m.group("date"), "%d%b%y").date()
    expiry_dt = dt.datetime(
        expiry_date.year,
        expiry_date.month,
        expiry_date.day,
        8,
        0,
        0,
        0,
        tzinfo=dt.timezone.utc,
    )
    return int(expiry_dt.timestamp() * 1000)


def _parse_deribit_cp(instrument_name: str) -> str | None:
    m = _DERIBIT_OPT_RE.match(instrument_name)
    if not m:
        return None
    t = m.group("type")
    return "C" if t == "C" else ("P" if t == "P" else None)


def _coerce_cp(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    u = s.upper()
    if u in {"C", "CALL"}:
        return "C"
    if u in {"P", "PUT"}:
        return "P"
    if u.startswith("CALL"):
        return "C"
    if u.startswith("PUT"):
        return "P"
    return None


# Any IV-ish column is treated as venue-fetched and kept only in aux.
_IV_KEY_RE = re.compile(r"^(iv)$|(_iv$)|(^iv_)|(_iv_)", re.IGNORECASE)


def _is_iv_col(c: str) -> bool:
    return bool(_IV_KEY_RE.search(c))


_GREEK_COLS = {
    "delta",
    "gamma",
    "vega",
    "theta",
    "rho",
    "vanna",
    "vomma",
    "volga",
    "charm",
    "speed",
    "zomma",
    "color",
}


_CHAIN_COLS = {
    "instrument_name",
    "expiration_timestamp",
    "expiry_ts",
    "strike",
    "option_type",
    "cp",
    "state",
    "is_active",
    "instrument_id",
    "settlement_currency",
    "base_currency",
    "quote_currency",
    "contract_size",
    "tick_size",
    "min_trade_amount",
    "kind",
    "instrument_type",
    "price_index",
    "counter_currency",
    "settlement_period",
    "tick_size_steps",
}

_QUOTE_COLS = {
    "instrument_name",
    "bid_price",
    "ask_price",
    "mid_price",
    "last",
    "mark_price",
    "open_interest",
    "volume_24h",
    "volume_usd_24h",
    "mark_iv",
    "high",
    "low",
    "market_ts",
    "price_change",
}

_UNDERLYING_COLS = {
    "instrument_name", ### for merge
    "underlying_price",
    "underlying_index", ## BTC/BTCUSDT
    "estimated_delivery_price",
    "interest_rate",
}


@dataclass(frozen=True)
class OptionChainSnapshot(Snapshot):
    """Immutable option chain snapshot.

    Schema v3:
      - chain_frame: instrument identity & lifecycle
      - quote_frame: core quotes
      - underlying_frame: underlying fields
      - aux_frame: all remaining fields
    """

    data_ts: int
    symbol: str
    market: MarketSpec
    domain: str
    schema_version: int

    chain_frame: pd.DataFrame
    quote_frame: pd.DataFrame
    underlying_frame: pd.DataFrame
    aux_frame: pd.DataFrame
    expiry_keys_ms: frozenset[int] | None = None
    term_keys_ms: dict[int, frozenset[int]] | None = None
    _frame_cache: pd.DataFrame | None = field(default=None, init=False, repr=False)
    _market_ts_ref: int | None = field(default=None, init=False, repr=False) # cache for market timestamp reference, i.e. the ts key reference

    # --- Row-level eligibility annotation (optional; set via _snapshot_with_annotation) ---
    row_flags: np.ndarray | None = field(default=None, repr=False)
    row_mask: np.ndarray | None = field(default=None, repr=False)
    row_policy_hash: str | None = None

    def __post_init__(self) -> None:
        rf = self.row_flags
        rm = self.row_mask
        has_flags = rf is not None
        has_mask = rm is not None
        if has_flags != has_mask:
            raise ValueError("row_flags and row_mask must both be set or both be None")
        if has_flags:
            if not isinstance(rf, np.ndarray) or rf.ndim != 1:
                raise TypeError("row_flags must be a 1-D numpy ndarray")
            if not np.issubdtype(rf.dtype, np.unsignedinteger):
                raise TypeError(f"row_flags dtype must be unsigned int, got {rf.dtype}")
            if len(rf) != len(self.chain_frame):
                raise ValueError(f"row_flags length {len(rf)} != chain_frame length {len(self.chain_frame)}")
        if has_mask:
            if not isinstance(rm, np.ndarray) or rm.ndim != 1:
                raise TypeError("row_mask must be a 1-D numpy ndarray")
            if rm.dtype != np.dtype("bool"):
                raise TypeError(f"row_mask dtype must be bool, got {rm.dtype}")
            if len(rm) != len(self.chain_frame):
                raise ValueError(f"row_mask length {len(rm)} != chain_frame length {len(self.chain_frame)}")

    @staticmethod
    def _sort_frame(df: pd.DataFrame) -> pd.DataFrame:
        ## Sort in a defined order for consistency
        ## Sort by expiration_timestamp/expiry_ts, strike, cp/option_type, instrument_name
        ## Use stable sort to preserve original ordering where possible
        if df is None or df.empty:
            return pd.DataFrame(columns=list(df.columns) if df is not None else [])
        order: list[str] = []
        if "expiration_timestamp" in df.columns:
            order.append("expiration_timestamp")
        elif "expiry_ts" in df.columns:
            order.append("expiry_ts")
        if "strike" in df.columns:
            order.append("strike")
        if "cp" in df.columns:
            order.append("cp")
        elif "option_type" in df.columns:
            order.append("option_type")
        if "instrument_name" in df.columns:
            order.append("instrument_name")
        if order:
            try:
                return df.sort_values(order, kind="stable").reset_index(drop=True)
            except Exception:
                pass
        if "instrument_name" in df.columns:
            try:
                return df.sort_values("instrument_name", kind="stable").reset_index(drop=True)
            except Exception:
                pass
        return df.reset_index(drop=True)

    @staticmethod
    def _split_frames(
        df: pd.DataFrame,
        *,
        drop_aux: bool,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        # Split a DataFrame into chain, quote, underlying, and aux frames
        # used for from_chain_aligned
        if df is None or df.empty:
            empty = pd.DataFrame(columns=["instrument_name"])
            return empty, empty, empty, empty

        x = df.copy()
        if "instrument_name" not in x.columns:
            x["instrument_name"] = None

        def _select(cols: set[str]) -> pd.DataFrame:
            wanted = [c for c in x.columns if c in cols]
            if "instrument_name" not in wanted:
                wanted = ["instrument_name"] + wanted
            return x[wanted].copy()

        chain_frame = _select(_CHAIN_COLS)
        quote_frame = _select(_QUOTE_COLS)
        underlying_frame = _select(_UNDERLYING_COLS)

        if "expiration_timestamp" in chain_frame.columns and "expiry_ts" not in chain_frame.columns:
            chain_frame["expiry_ts"] = pd.to_numeric(chain_frame["expiration_timestamp"], errors="coerce")
        if "option_type" in chain_frame.columns and "cp" not in chain_frame.columns:
            chain_frame["cp"] = chain_frame["option_type"].map(_coerce_cp)

        if drop_aux:
            aux_frame = pd.DataFrame({"instrument_name": x["instrument_name"]})
        else:
            used = set(chain_frame.columns) | set(quote_frame.columns) | set(underlying_frame.columns)
            aux_cols = [c for c in x.columns if c not in used]
            if "instrument_name" not in aux_cols:
                aux_cols = ["instrument_name"] + aux_cols
            aux_frame = x[aux_cols].copy() if aux_cols else x[["instrument_name"]].copy()

        # Sort chain_frame (the authority for row order)
        chain_frame = OptionChainSnapshot._sort_frame(chain_frame)

        # Align other sub-frames to chain_frame's instrument_name order (O(n) reindex)
        if "instrument_name" in chain_frame.columns and len(chain_frame) > 0:
            _name_order = chain_frame["instrument_name"]
            quote_frame = (
                quote_frame.set_index("instrument_name")
                .reindex(_name_order)
                .reset_index()
            )
            underlying_frame = (
                underlying_frame.set_index("instrument_name")
                .reindex(_name_order)
                .reset_index()
            )
            aux_frame = (
                aux_frame.set_index("instrument_name")
                .reindex(_name_order)
                .reset_index()
            )
        else:
            quote_frame = OptionChainSnapshot._sort_frame(quote_frame)
            underlying_frame = OptionChainSnapshot._sort_frame(underlying_frame)
            aux_frame = OptionChainSnapshot._sort_frame(aux_frame)

        return chain_frame, quote_frame, underlying_frame, aux_frame

    @classmethod
    def from_chain_aligned(
        cls,
        *,
        data_ts: int,
        symbol: str,
        market: MarketSpec | None = None,
        chain: pd.DataFrame,
        drop_aux: bool | None = None,
        schema_version: int = 3,
        domain: str = "option_chain",
    ) -> "OptionChainSnapshot":
        # This is the core constructor from aligned DataFrame
        dts = to_ms_int(data_ts)

        if not isinstance(chain, pd.DataFrame):
            raise TypeError("OptionChainSnapshot.from_chain_aligned expects `chain` as a pandas DataFrame")

        if drop_aux is None:
            drop_aux = DROP_AUX_DEFAULT
        chain_frame, quote_frame, underlying_frame, aux_frame = cls._split_frames(chain, drop_aux=bool(drop_aux))

        expiry_series = None
        if "expiration_timestamp" in chain_frame.columns:
            expiry_series = pd.to_numeric(chain_frame["expiration_timestamp"], errors="coerce")
        elif "expiry_ts" in chain_frame.columns:
            expiry_series = pd.to_numeric(chain_frame["expiry_ts"], errors="coerce")

        expiry_keys_ms: frozenset[int] | None
        if expiry_series is not None:
            try:
                xs = expiry_series.dropna().astype("int64")
                expiry_keys_ms = frozenset(int(v) for v in xs.unique() if int(v) > 0)
            except Exception:
                expiry_keys_ms = frozenset()
        else:
            expiry_keys_ms = frozenset()

        return cls(
            data_ts=dts,
            symbol=symbol,
            market=ensure_market_spec(market),
            domain=domain,
            schema_version=int(schema_version),
            chain_frame=chain_frame,
            quote_frame=quote_frame,
            underlying_frame=underlying_frame,
            aux_frame=aux_frame,
            expiry_keys_ms=expiry_keys_ms,
            term_keys_ms=None,
        )

    @classmethod
    def from_chain(
        cls,
        ts: int,
        chain: pd.DataFrame,
        symbol: str,
        market: MarketSpec | None = None,
    ) -> "OptionChainSnapshot":
        return cls.from_chain_aligned(
            data_ts=ts,
            symbol=symbol,
            market=market,
            chain=chain,
        )

    def to_dict(self) -> Dict[str, Any]:
        assert isinstance(self.market, MarketInfo)
        return {
            "data_ts": self.data_ts,
            "symbol": self.symbol,
            "market": self.market.to_dict(),
            "domain": self.domain,
            "schema_version": self.schema_version,
            # store as records for JSON-compat
            "frame": self.frame,
        }
    
    def get_attr(self, key: str) -> Any:
        # Generic getter for dynamic attribute access, covered with tests
        if not hasattr(self, key):
            raise AttributeError(f"{type(self).__name__} has no attribute {key!r}")
        return getattr(self, key)

    def get_expiry_keys_ms(self) -> frozenset[int]:
        # expiry_keys_ms cached against immutable chain_frame; chain_frame must not change post creation.
        cached = self.expiry_keys_ms
        if cached is not None:
            return cached
        frame = self.chain_frame
        if frame is None or frame.empty:
            keys = frozenset()
        else:
            try:
                ## no bucketing here, just unique expiry timestamps
                if "expiration_timestamp" in frame.columns:
                    xs = frame["expiration_timestamp"].dropna().unique()
                elif "expiry_ts" in frame.columns:
                    xs = frame["expiry_ts"].dropna().unique()
                else:
                    xs = []
                keys = frozenset(int(v) for v in xs if v is not None and int(v) > 0)
            except Exception:
                keys = frozenset()
        object.__setattr__(self, "expiry_keys_ms", keys)
        return keys

    def get_term_keys_ms(self, term_bucket_ms: int) -> frozenset[int]:
        ## getting term keys for a given bucket size
        tb = int(term_bucket_ms)
        if tb <= 0:
            raise ValueError("term_bucket_ms must be > 0")
        cached = self.term_keys_ms
        if cached is None:
            cached = {}
        if tb in cached:
            return cached[tb]
        # term_keys_ms cached against immutable data_ts; data_ts must not change post creation.
        snap_ts = int(self.data_ts)
        keys = {
            (max(0, int(ex) - snap_ts) // tb) * tb
            for ex in self.get_expiry_keys_ms()
        }
        out = frozenset(int(k) for k in keys)
        cached[tb] = out
        if len(cached) > 4:
            cached.pop(next(iter(cached)))
        object.__setattr__(self, "term_keys_ms", cached)
        return out

    def get_market_ts_ref(self) -> int | None:
        # market_ts_ref cached against immutable quote_frame; quote_frame must not change post creation.
        # the reference (the key name) for the market timestamp
        cached = getattr(self, "_market_ts_ref", None) 
        if cached is not None:
            return cached
        quote = self.quote_frame
        if quote is None or quote.empty or "market_ts" not in quote.columns:
            return None
        xs = pd.to_numeric(quote["market_ts"], errors="coerce").dropna()
        if xs.empty:
            return None
        ref = int(xs.median())
        object.__setattr__(self, "_market_ts_ref", ref)
        return ref

    @property
    def frame(self) -> pd.DataFrame:
        # merging chain, quote and underlying frames for a comprehensive view;
        # aux is intentionally left out of this merge.
        cached = getattr(self, "_frame_cache", None)
        if cached is not None:
            return cached.copy(deep=False)  # enforce snapshot immutability contract for callers 
        base = self.chain_frame
        if base is None or base.empty:
            merged = pd.DataFrame()
        else:
            quote = self.quote_frame
            if quote is None or quote.empty:
                merged = base.copy()
            else:
                merged = base.merge(quote, on="instrument_name", how="left", suffixes=("", "_quote"))
            underlying = self.underlying_frame
            if underlying is not None and not underlying.empty:
                merged = merged.merge(underlying, on="instrument_name", how="left", suffixes=("", "_underlying"))
        object.__setattr__(self, "_frame_cache", merged)
        return merged.copy(deep=False)  # enforce snapshot immutability contract for callers  # +


def _snapshot_with_annotation(
    snap: OptionChainSnapshot,
    *,
    row_flags: np.ndarray,
    row_mask: np.ndarray,
    row_policy_hash: str,
) -> OptionChainSnapshot:
    """Return a new snapshot with row-level annotation attached.

    Uses dataclasses.replace on the frozen dataclass; resets internal caches
    so they recompute lazily on the new instance.
    """
    out = _dc_replace(
        snap,
        row_flags=row_flags,
        row_mask=row_mask,
        row_policy_hash=row_policy_hash,
    )
    object.__setattr__(out, "_frame_cache", None)
    object.__setattr__(out, "_market_ts_ref", None)
    return out


def _empty_frame_like(frame: pd.DataFrame) -> pd.DataFrame:
    # construct an empty DataFrame with the same columns as the given frame, if possible; otherwise return a generic empty DataFrame
    # this is used as a fallback when slicing fails or when the input frame is None/empty; 
    if frame is None:
        return pd.DataFrame()
    try:
        return frame.iloc[0:0].copy()
    except Exception:
        return pd.DataFrame(columns=list(frame.columns) if hasattr(frame, "columns") else [])


def _slice_frame_for_expiry(frame: pd.DataFrame, *, expiry_ts: int) -> pd.DataFrame:
    if frame is None or frame.empty:
        return _empty_frame_like(frame)
    if "expiry_ts" not in frame.columns and "expiration_timestamp" not in frame.columns:
        return _empty_frame_like(frame)
    try:
        col = "expiry_ts" if "expiry_ts" in frame.columns else "expiration_timestamp"
        mask = pd.to_numeric(frame[col], errors="coerce").fillna(0).astype("int64") == int(expiry_ts)
        return frame.loc[mask].reset_index(drop=True)
    except Exception:
        return _empty_frame_like(frame)


def _slice_frame_for_term_bucket(
    frame: pd.DataFrame,
    *,
    snap_ts: int,
    term_key_ms: int,
    term_bucket_ms: int,
) -> pd.DataFrame:
    # Slice any DataFrame for options within a specific term bucket
    if frame is None or frame.empty:
        return _empty_frame_like(frame)
    if "expiry_ts" not in frame.columns and "expiration_timestamp" not in frame.columns:
        return _empty_frame_like(frame)
    try:
        col = "expiry_ts" if "expiry_ts" in frame.columns else "expiration_timestamp"
        expiry = pd.to_numeric(frame[col], errors="coerce").fillna(0).astype("int64")
        term = (expiry - int(snap_ts)).clip(lower=0)
        key = (term // int(term_bucket_ms)) * int(term_bucket_ms)
        mask = key == int(term_key_ms)
        return frame.loc[mask].reset_index(drop=True)
    except Exception:
        return _empty_frame_like(frame)


class OptionChainSnapshotView(OptionChainSnapshot):
    """Lazy view over an OptionChainSnapshot frame.

    Frame invariant: market-only columns; provenance/selection live on the view object, not in the DataFrame.
    """
    """Only for one slice at a time (by expiry or term bucket)."""
    ## Desinged for efficient slicing by expiry or term bucket without copying the entire frame upfront;
    ## the frame is filtered on demand and cached for subsequent accesses;
    _base: OptionChainSnapshot
    _frame_filter: Callable[[pd.DataFrame], pd.DataFrame] # e.g. _slice_frame_for_expiry
    _frame_cache: pd.DataFrame | None  

    def __init__(
        self,
        *,
        base: OptionChainSnapshot,
        frame_filter: Callable[[pd.DataFrame], pd.DataFrame],
        slice_kind: str | None = None,
        slice_key: int | None = None,
        snapshot_market_ts: int | None = None,
        selection: dict[str, Any] | None = None,
    ):
        object.__setattr__(self, "_base", base)
        object.__setattr__(self, "_frame_filter", frame_filter)
        object.__setattr__(self, "_frame_cache", None)

        object.__setattr__(self, "data_ts", base.data_ts) # arrival timestamp
        object.__setattr__(self, "snapshot_data_ts", int(base.data_ts)) # provenance attribute (not a frame column)
        object.__setattr__(self, "symbol", base.symbol)
        object.__setattr__(self, "market", base.market)
        object.__setattr__(self, "domain", base.domain)
        object.__setattr__(self, "schema_version", base.schema_version)

        for k, v in getattr(base, "__dict__", {}).items():
            if k in {"data_ts", "symbol", "market", "domain", "schema_version"}:
                continue
            if k.startswith("_"):
                continue
            try:
                object.__setattr__(self, k, v)
            except Exception:
                pass
        object.__setattr__(self, "slice_kind", slice_kind) # e.g. "expiry" or "term_bucket"
        object.__setattr__(self, "slice_key", slice_key) # e.g. expiry_ts or term_key_ms
        if snapshot_market_ts is None:
            snapshot_market_ts = base.get_market_ts_ref()
        object.__setattr__(self, "snapshot_market_ts", snapshot_market_ts) # provenance attribute (not a frame column)
        object.__setattr__(self, "selection", selection)

    @property
    def frame(self) -> pd.DataFrame:
        cached = getattr(self, "_frame_cache", None)
        if cached is None:
            try:
                view = self._frame_filter(self._base.frame)
            except Exception:
                view = self._base.frame
            view = view.copy()
            # Keep frame market-only; provenance and selection live on the view object.
            object.__setattr__(self, "_frame_cache", view)
            return view
        return cached

    @classmethod
    def for_expiry(cls, *, base: OptionChainSnapshot, expiry_ts: int) -> "OptionChainSnapshotView":
        ex = int(expiry_ts)
        if ex not in base.get_expiry_keys_ms():
            return cls(base=base, frame_filter=lambda f: _empty_frame_like(f), slice_kind="expiry", slice_key=ex)
        return cls(base=base, frame_filter=lambda f: _slice_frame_for_expiry(f, expiry_ts=ex), slice_kind="expiry", slice_key=ex)

    @classmethod
    def for_term_bucket(
        cls,
        *,
        base: OptionChainSnapshot,
        term_key_ms: int,
        term_bucket_ms: int,
    ) -> "OptionChainSnapshotView":
        snap_ts = int(base.data_ts)
        tk = int(term_key_ms)
        tb = int(term_bucket_ms)
        if tk not in base.get_term_keys_ms(tb):
            return cls(base=base, frame_filter=lambda f: _empty_frame_like(f), slice_kind="term_bucket", slice_key=tk)
        return cls(
            base=base,
            frame_filter=lambda f: _slice_frame_for_term_bucket(
                f,
                snap_ts=snap_ts,
                term_key_ms=tk,
                term_bucket_ms=tb,
            ),
            slice_kind="term_bucket",
            slice_key=tk,
        )
