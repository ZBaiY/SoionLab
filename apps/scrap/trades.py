"""Trades backfill for Binance aggTrades.

Notes:
- Event time is exchange-provided `data_ts` (epoch ms).
- Chunking/pagination here is IO/backfill-only and must not be confused with
  strategy observation intervals or runtime stepping.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Literal, cast
from pathlib import Path

import datetime as _dt
import pandas as pd
import numpy as np
import requests
import argparse
import signal
import threading
from tqdm import tqdm

from quant_engine.utils.paths import data_root_from_file, resolve_under_root
from quant_engine.utils.parquet_store import write_raw_snapshot_append_only


_I64_MIN = np.iinfo("int64").min
POLL_INTERVAL = 60_000*5  # ms
DATA_ROOT = data_root_from_file(__file__, levels_up=2)

# ======================================================================================
# time helpers
# ======================================================================================

def _coerce_epoch_ms(x: Any) -> int:
    """Coerce timestamp-like input into epoch milliseconds int."""
    if x is None:
        raise TypeError("timestamp cannot be None")
    if isinstance(x, bool):
        raise TypeError("timestamp cannot be bool")

    if isinstance(x, int):
        # heuristic: seconds -> ms
        return x * 1000 if x < 10_000_000_000 else x

    if isinstance(x, float):
        # heuristic: seconds -> ms
        if x < 10_000_000_000:
            return int(round(x * 1000))
        return int(round(x))

    if isinstance(x, (pd.Timestamp, _dt.datetime)):
        ts = x
        if isinstance(ts, _dt.datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=_dt.timezone.utc)
        if isinstance(ts, pd.Timestamp):
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            return int(ts.value // 1_000_000)
        return int(ts.timestamp() * 1000)

    if isinstance(x, str):
        ts = pd.to_datetime(x, utc=True)
        return int(ts.value // 1_000_000)

    raise TypeError(f"Unsupported timestamp type: {type(x).__name__}")


def _as_int(x: Any) -> int:
    """Coerce pandas/numpy scalars (and friends) to built-in int (pylance-friendly)."""
    if isinstance(x, int) and not isinstance(x, bool):
        return x
    if isinstance(x, (str, bytes, bytearray)):
        return int(x)

    item = getattr(x, "item", None)
    if callable(item):
        try:
            v = item()
            if v is not x:
                return _as_int(v)
        except Exception:
            pass

    return int(cast("int | float | str | bytes | bytearray", x))


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _dt_series_to_epoch_ms(dt_utc: pd.Series) -> pd.Series:
    """
    dt_utc: Series[datetime-like] (anything pd.to_datetime can coerce)
    returns: Series[int64] epoch-ms, NaT -> 0
    """
    s = pd.to_datetime(dt_utc, utc=True, errors="coerce")
    ns = s.astype("int64")  # NaT -> int64 min
    ms = ns // 1_000_000
    ms = ms.where(ns != _I64_MIN, 0).astype("int64")
    return ms


# ======================================================================================
# schema normalization
# ======================================================================================

def _normalize_trades_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stable schema contract:
      - data_ts: int64 epoch-ms
      - agg_trade_id: int64
      - price: float64
      - qty: float64
      - is_buyer_maker: bool
      - time: datetime64[ns, UTC] (inspection)
    """
    out = df.copy()

    if "data_ts" in out.columns:
        out["data_ts"] = pd.to_numeric(out["data_ts"], errors="coerce")
    elif "time" in out.columns:
        s_utc = pd.to_datetime(out["time"], utc=True, errors="coerce")
        out["data_ts"] = _dt_series_to_epoch_ms(s_utc)
        out["time"] = s_utc
    else:
        out["data_ts"] = 0

    if "time" not in out.columns:
        # keep inspection column (optional)
        out["time"] = pd.to_datetime(out["data_ts"].astype("int64"), unit="ms", utc=True)

    if "agg_trade_id" in out.columns:
        out["agg_trade_id"] = pd.to_numeric(out["agg_trade_id"], errors="coerce")
    if "price" in out.columns:
        out["price"] = pd.to_numeric(out["price"], errors="coerce")
    if "qty" in out.columns:
        out["qty"] = pd.to_numeric(out["qty"], errors="coerce")

    # enforce ints (after Na handling in cleaner)
    out = out.sort_values(["data_ts", "agg_trade_id"], kind="stable")
    if "agg_trade_id" in out.columns:
        out = out.drop_duplicates(subset=["agg_trade_id"], keep="last")
    else:
        out = out.drop_duplicates(subset=["data_ts"], keep="last")

    out = out.reset_index(drop=True)
    return out


def concat_chunks(chunks: Iterable[pd.DataFrame], *, dedup: bool = True) -> pd.DataFrame:
    parts = [c for c in chunks if c is not None and not c.empty]
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    df = _normalize_trades_df(df)
    if dedup and "agg_trade_id" in df.columns:
        df = df.drop_duplicates(subset=["agg_trade_id"], keep="last")
    return df.reset_index(drop=True)


# ======================================================================================
# configs
# ======================================================================================

@dataclass(frozen=True)
class BinanceTradesFetcherConfig:
    base_url: str = "https://api.binance.com"
    endpoint: str = "/api/v3/aggTrades"  # use aggTrades for historical backfill
    timeout_s: float = 30.0
    max_limit: int = 1000


@dataclass(frozen=True)
class BinanceTradesCleanerConfig:
    strict: bool = True
    dropna: bool = True
    drop_glitch: bool = True
    allow_empty: bool = True


@dataclass(frozen=True)
class BinanceTradesBackfillConfig:
    symbol: str
    start_ms: int | float | str | _dt.datetime | pd.Timestamp
    end_ms: int | float | str | _dt.datetime | pd.Timestamp

    limit: int | None = None
    # IO-only storage sampling cadence (ms). Use 0 to disable.
    # Grid sampling fetches one trade per poll window.
    poll_interval_ms: int = POLL_INTERVAL

    write_raw: bool = True
    write_cleaned: bool = True

    strict: bool = False
    dropna: bool = True
    drop_glitch: bool = True


@dataclass(frozen=True)
class TradesParquetStoreConfig:
    """
    Layout:
      <root>/<stage>/trades/<symbol>/<interval>/<YYYY>/<YYYY_MM_DD>/<data_ts>.parquet
    """
    root: str = str(DATA_ROOT)
    domain: str = "trades"


@dataclass(frozen=True)
class TradesCleanReport:
    n_in: int
    n_out: int
    dup_dropped: int
    rows_dropped_na: int
    glitch_dropped: int
    non_monotonic_id: int
    non_monotonic_time: int
    first_data_ts: int | None
    last_data_ts: int | None
    first_id: int | None
    last_id: int | None

    def summary(self) -> str:
        return "\n".join(
            [
                "Trades Clean Report:",
                f"  Input rows: {self.n_in}",
                f"  Output rows: {self.n_out}",
                f"  Duplicates dropped: {self.dup_dropped}",
                f"  Rows dropped (NA): {self.rows_dropped_na}",
                f"  Glitch rows dropped: {self.glitch_dropped}",
                f"  Non-monotonic agg_trade_id: {self.non_monotonic_id}",
                f"  Non-monotonic data_ts: {self.non_monotonic_time}",
                f"  First data_ts: {self.first_data_ts}",
                f"  Last data_ts: {self.last_data_ts}",
                f"  First agg_trade_id: {self.first_id}",
                f"  Last agg_trade_id: {self.last_id}",
            ]
        )


# ======================================================================================
# fetcher
# ======================================================================================

class BinanceTradesFetcher:
    """
    Fetch Binance aggTrades and normalize to:
      - data_ts: int64 ms
      - time: datetime64[ns, UTC]
      - agg_trade_id: int64
      - price: float
      - qty: float
      - first_trade_id: int64
      - last_trade_id: int64
      - is_buyer_maker: bool
      - ignore: bool
    """

    def __init__(self, *, cfg: BinanceTradesFetcherConfig | None = None, session: requests.Session | None = None):
        self._cfg = cfg or BinanceTradesFetcherConfig()
        self._session = session or requests.Session()

    def _url(self) -> str:
        return f"{self._cfg.base_url}{self._cfg.endpoint}"

    def fetch_chunk(
        self,
        *,
        symbol: str,
        start_ms: int | float | str | _dt.datetime | pd.Timestamp | None = None,
        end_ms: int | float | str | _dt.datetime | pd.Timestamp | None = None,
        from_id: int | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        lim = int(limit or self._cfg.max_limit)
        if lim <= 0 or lim > self._cfg.max_limit:
            raise ValueError(f"limit must be in [1, {self._cfg.max_limit}]")

        params: dict[str, Any] = {"symbol": symbol, "limit": lim}

        if from_id is not None:
            params["fromId"] = int(from_id)
        else:
            if start_ms is None:
                raise ValueError("start_ms required when from_id is None")
            params["startTime"] = _coerce_epoch_ms(start_ms)

        if end_ms is not None:
            params["endTime"] = _coerce_epoch_ms(end_ms)
        r = self._session.get(self._url(), params=params, timeout=self._cfg.timeout_s)
        r.raise_for_status()
        payload = r.json()
        if not payload:
            return pd.DataFrame()
        # Binance aggTrades keys: a,p,q,f,l,T,m,M
        # map to stable columns
        df = pd.DataFrame(payload)
        # pause here to inspect        
        # rename + keep only what we want
        rename = {
            "a": "agg_trade_id",
            "p": "price",
            "q": "qty",
            "f": "first_trade_id",
            "l": "last_trade_id",
            "T": "data_ts",
            "m": "is_buyer_maker",
            "M": "ignore",
        }
        df = df.rename(columns=rename)
        
        # dtypes
        for c in ("agg_trade_id", "first_trade_id", "last_trade_id", "data_ts"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        for c in ("price", "qty"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        if "is_buyer_maker" in df.columns:
            df["is_buyer_maker"] = df["is_buyer_maker"].astype("bool")
        if "ignore" in df.columns:
            df["ignore"] = df["ignore"].astype("bool")

        df["time"] = pd.to_datetime(df["data_ts"], unit="ms", utc=True)

        front = ["data_ts", "time", "agg_trade_id"]
        rest = [c for c in df.columns if c not in front]
        df = df[front + rest]

        # normalize & dedup
        df = _normalize_trades_df(df)

        # enforce ints (post-normalize)
        df["data_ts"] = df["data_ts"].astype("int64")
        if "agg_trade_id" in df.columns:
            df["agg_trade_id"] = df["agg_trade_id"].astype("int64")
        if "first_trade_id" in df.columns:
            df["first_trade_id"] = df["first_trade_id"].astype("int64")
        if "last_trade_id" in df.columns:
            df["last_trade_id"] = df["last_trade_id"].astype("int64")

        return df

# ======================================================================================
# cleaner
# ======================================================================================

class BinanceTradesCleaner:
    _REQ = ("data_ts", "agg_trade_id", "price", "qty")

    def __init__(self, *, cfg: BinanceTradesCleanerConfig):
        self._cfg = cfg

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, TradesCleanReport]:
        if df is None or len(df) == 0:
            if self._cfg.allow_empty:
                rep = TradesCleanReport(
                    n_in=0, n_out=0,
                    dup_dropped=0,
                    rows_dropped_na=0,
                    glitch_dropped=0,
                    non_monotonic_id=0,
                    non_monotonic_time=0,
                    first_data_ts=None, last_data_ts=None,
                    first_id=None, last_id=None,
                )
                return pd.DataFrame(), rep
            raise ValueError("Empty trades frame")

        missing = [c for c in self._REQ if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        n_in = int(len(df))

        x = _normalize_trades_df(df)

        # numeric coercion (again; stable)
        x["data_ts"] = pd.to_numeric(x["data_ts"], errors="coerce")
        x["agg_trade_id"] = pd.to_numeric(x["agg_trade_id"], errors="coerce")
        x["price"] = pd.to_numeric(x["price"], errors="coerce")
        x["qty"] = pd.to_numeric(x["qty"], errors="coerce")

        rows_dropped_na = 0
        if self._cfg.dropna:
            before = len(x)
            x = x.dropna(subset=list(self._REQ))
            rows_dropped_na = before - len(x)

        glitch_dropped = 0
        if self._cfg.drop_glitch and len(x) > 0:
            before = len(x)
            # common glitch signatures:
            #   - price <= 0
            #   - qty <= 0 (spot aggTrades qty should be positive)
            glitch = (x["price"] <= 0) | (x["qty"] <= 0)
            glitch_dropped = int(glitch.sum())
            if glitch_dropped:
                x = x.loc[~glitch].copy()

        # enforce ints
        x["data_ts"] = x["data_ts"].astype("int64")
        x["agg_trade_id"] = x["agg_trade_id"].astype("int64")

        # dedup by id
        before = len(x)
        x = x.sort_values(["agg_trade_id", "data_ts"], kind="stable")
        x = x.drop_duplicates(subset=["agg_trade_id"], keep="last")
        dup_dropped = before - len(x)

        # monotonic diagnostics (not fatal unless strict)
        non_mono_id = 0
        non_mono_ts = 0
        if len(x) >= 2:
            non_mono_id = int((x["agg_trade_id"].diff().iloc[1:] <= 0).sum())
            non_mono_ts = int((x["data_ts"].diff().iloc[1:] < 0).sum())

        # sort final for storage
        x = x.sort_values(["data_ts", "agg_trade_id"], kind="stable").reset_index(drop=True)

        if self._cfg.strict:
            if non_mono_id != 0:
                raise ValueError(f"Non-monotonic agg_trade_id detected: {non_mono_id}")
            if non_mono_ts != 0:
                raise ValueError(f"Non-monotonic data_ts detected: {non_mono_ts}")

        n_out = int(len(x))
        rep = TradesCleanReport(
            n_in=n_in,
            n_out=n_out,
            dup_dropped=int(dup_dropped),
            rows_dropped_na=int(rows_dropped_na),
            glitch_dropped=int(glitch_dropped),
            non_monotonic_id=int(non_mono_id),
            non_monotonic_time=int(non_mono_ts),
            first_data_ts=int(x["data_ts"].iloc[0]) if n_out else None,
            last_data_ts=int(x["data_ts"].iloc[-1]) if n_out else None,
            first_id=int(x["agg_trade_id"].iloc[0]) if n_out else None,
            last_id=int(x["agg_trade_id"].iloc[-1]) if n_out else None,
        )
        return x, rep


# ======================================================================================
# store (append-only snapshots)
# ======================================================================================

class TradesParquetStore:
    def __init__(self, *, cfg: TradesParquetStoreConfig | None = None):
        self._cfg = cfg or TradesParquetStoreConfig()

    def write_chunk(
        self,
        df: pd.DataFrame,
        *,
        stage: Literal["raw", "cleaned"],
        symbol: str,
    ) -> list[Path]:
        if df is None or df.empty:
            return []
        if "data_ts" not in df.columns:
            raise ValueError("write_chunk requires data_ts")
        x = _normalize_trades_df(df.copy())
        # dedup by agg_trade_id within the chunk
        if "agg_trade_id" in x.columns:
            x = x.drop_duplicates(subset=["agg_trade_id"], keep="last")
            x = x.sort_values(["data_ts", "agg_trade_id"], kind="stable").reset_index(drop=True)
        else:
            x = x.drop_duplicates(subset=["data_ts"], keep="last")
            x = x.sort_values(["data_ts"], kind="stable").reset_index(drop=True)

        written: list[Path] = []
        for ts, sub in x.groupby("data_ts", sort=True):
            out = write_raw_snapshot_append_only(
                root=Path(self._cfg.root),
                domain=self._cfg.domain,
                asset=symbol,
                interval="trades",
                data_ts=int(ts),
                df=sub,
                sort_cols=["data_ts", "agg_trade_id"] if "agg_trade_id" in sub.columns else ["data_ts"],
                stage=stage,
            )
            written.append(out)

        return written


# ======================================================================================
# backfiller
# ======================================================================================

class BinanceTradesBackfiller:
    def __init__(self, *, fetcher: BinanceTradesFetcher | None = None, store: TradesParquetStore | None = None):
        self._fetcher = fetcher or BinanceTradesFetcher()
        self._store = store or TradesParquetStore()

    def run(self, *, cfg: BinanceTradesBackfillConfig, stop_event: threading.Event | None = None) -> dict[str, Any]:
        cleaner = BinanceTradesCleaner(
            cfg=BinanceTradesCleanerConfig(
                strict=cfg.strict,
                dropna=cfg.dropna,
                drop_glitch=cfg.drop_glitch,
                allow_empty=True,
            )
        )

        totals = {
            "chunks": 0,
            "raw_rows": 0,
            "clean_rows": 0,
            "rows_dropped_na": 0,
            "glitch_dropped": 0,
            "dup_dropped": 0,
            "written_raw_files": 0,
            "written_cleaned_files": 0,
            "non_monotonic_id": 0,
            "non_monotonic_time": 0,
        }

        if not cfg.poll_interval_ms or cfg.poll_interval_ms <= 0:
            raise ValueError("poll_interval_ms must be > 0 for grid sampling")

        chunks = _iter_grid_samples(
            fetcher=self._fetcher,
            symbol=cfg.symbol,
            start_ms=cfg.start_ms,
            end_ms=cfg.end_ms,
            poll_interval_ms=int(cfg.poll_interval_ms),
            stop_event=stop_event,
        )

        for chunk in chunks:
            totals["chunks"] += 1
            totals["raw_rows"] += int(len(chunk))

            if cfg.write_raw:
                w = self._store.write_chunk(chunk, stage="raw", symbol=cfg.symbol)
                totals["written_raw_files"] += len(w)

            if cfg.write_cleaned:
                cleaned, rep = cleaner.clean(chunk)
                totals["clean_rows"] += int(len(cleaned))
                totals["rows_dropped_na"] += int(rep.rows_dropped_na)
                totals["glitch_dropped"] += int(rep.glitch_dropped)
                totals["dup_dropped"] += int(rep.dup_dropped)
                totals["non_monotonic_id"] += int(rep.non_monotonic_id)
                totals["non_monotonic_time"] += int(rep.non_monotonic_time)

                w = self._store.write_chunk(cleaned, stage="cleaned", symbol=cfg.symbol)
                totals["written_cleaned_files"] += len(w)

        return totals


# ======================================================================================
# main (defaults-first)
# ======================================================================================

def _month_starts(start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> list[pd.Timestamp]:
    # inclusive months
    s = pd.Timestamp(year=start_ts.year, month=start_ts.month, day=1, tz="UTC")
    e = pd.Timestamp(year=end_ts.year, month=end_ts.month, day=1, tz="UTC")
    months = []
    cur = s
    while cur <= e:
        months.append(cur)
        # add 1 month
        y = cur.year + (cur.month // 12)
        m = (cur.month % 12) + 1
        cur = pd.Timestamp(year=y, month=m, day=1, tz="UTC")
    return months


def _iter_grid_samples(
    *,
    fetcher: BinanceTradesFetcher,
    symbol: str,
    start_ms: int | float | str | _dt.datetime | pd.Timestamp,
    end_ms: int | float | str | _dt.datetime | pd.Timestamp,
    poll_interval_ms: int,
    stop_event: threading.Event | None = None,
) -> Iterator[pd.DataFrame]:
    start = _coerce_epoch_ms(start_ms)
    end = _coerce_epoch_ms(end_ms)
    step = int(poll_interval_ms)
    if step <= 0:
        return

    cur = start
    last_id: int | None = None
    while cur < end:
        if stop_event is not None and stop_event.is_set():
            return
        df = fetcher.fetch_chunk(
            symbol=symbol,
            start_ms=cur,
            end_ms=None,
            limit=1,
        )
        if df is None or df.empty:
            cur += step
            continue

        trade_id: int | None = None
        trade_ts: int | None = None
        if "agg_trade_id" in df.columns:
            try:
                trade_id = int(df["agg_trade_id"].iloc[-1])
            except Exception:
                trade_id = None
        if "data_ts" in df.columns:
            try:
                trade_ts = int(df["data_ts"].iloc[-1])
            except Exception:
                trade_ts = None

        if trade_id is not None and last_id is not None and trade_id <= last_id:
            # Advance past the repeated trade to avoid getting stuck on gaps.
            if trade_ts is not None:
                cur = max(cur + step, trade_ts + 1)
            else:
                cur += step
            continue

        if trade_id is not None:
            last_id = trade_id

        yield df

        if trade_ts is not None:
            cur = max(cur + step, trade_ts + 1)
        else:
            cur += step


def main() -> None:
    parser = argparse.ArgumentParser(description="Trades backfill (Binance aggTrades)")
    parser.add_argument("--root", default=str(DATA_ROOT), help="data root")
    parser.add_argument("--symbols", default="BTCUSDT", help="comma-separated, e.g. BTCUSDT,ETHUSDT")
    parser.add_argument("--start", default="2025-01-01 00:00:00+00:00")
    parser.add_argument("--end", default=None, help="default: now (UTC)")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--poll_interval_ms", type=int, default=60_000 * 100, help="grid sampling cadence (ms)")
    parser.add_argument("--write_raw", action="store_true", default=False)
    parser.add_argument("--strict", action="store_true", default=False)
    args = parser.parse_args()

    stop_event = threading.Event()
    def _handle_stop(signum, _frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    # end_ts: utcnow already tz-aware in newer pandas; still keep robust handling
    end_ts = pd.Timestamp.utcnow()
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize("UTC")
    else:
        end_ts = end_ts.tz_convert("UTC")

    end_str = args.end or end_ts.strftime("%Y-%m-%d %H:%M:%S+00:00")
    start_ts = pd.to_datetime(args.start, utc=True)
    end_ts2 = pd.to_datetime(end_str, utc=True)

    root = resolve_under_root(DATA_ROOT, args.root, strip_prefix="data")
    store = TradesParquetStore(cfg=TradesParquetStoreConfig(root=str(root), domain="trades"))
    bf = BinanceTradesBackfiller(store=store)

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    months = _month_starts(start_ts, end_ts2)

    for sym in symbols:
        if stop_event.is_set():
            break
        pbar = tqdm(months, desc=f"backfill {sym} trades", unit="month")
        for m0 in pbar:
            if stop_event.is_set():
                break
            # month window [m0, m1)
            y = m0.year + (m0.month // 12)
            mo = (m0.month % 12) + 1
            m1 = pd.Timestamp(year=y, month=mo, day=1, tz="UTC")

            s = max(start_ts, m0)
            e = min(end_ts2, m1)
            if s >= e:
                continue

            totals = bf.run(
                cfg=BinanceTradesBackfillConfig(
                    symbol=sym,
                    start_ms=s,
                    end_ms=e,
                    limit=args.limit,
                    poll_interval_ms=args.poll_interval_ms,
                    write_raw=args.write_raw,
                    write_cleaned=True,
                    strict=args.strict,
                    dropna=True,
                    drop_glitch=True,
                ),
                stop_event=stop_event,
            )
            pbar.set_postfix(
                chunks=totals["chunks"],
                raw=totals["raw_rows"],
                clean=totals["clean_rows"],
                glitch=totals["glitch_dropped"],
                dup=totals["dup_dropped"],
            )
        if stop_event.is_set():
            break


if __name__ == "__main__":
    main()
