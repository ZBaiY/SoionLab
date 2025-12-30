from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Literal, cast
from pathlib import Path

import datetime as _dt
import pandas as pd
import numpy as np
import requests
import argparse
import os
import signal
import threading
from tqdm import tqdm

from quant_engine.utils.paths import data_root_from_file, resolve_under_root

_I64_MIN = np.iinfo("int64").min

DATA_ROOT = data_root_from_file(__file__, levels_up=2)

def _coerce_epoch_ms(x: Any) -> int:
    """Coerce timestamp-like input into epoch milliseconds int."""
    if x is None:
        raise TypeError("timestamp cannot be None")

    if isinstance(x, bool):
        raise TypeError("timestamp cannot be bool")

    if isinstance(x, int):
        # Heuristic: if user passed seconds, upscale.
        return x * 1000 if x < 10_000_000_000 else x

    if isinstance(x, float):
        # If float seconds, upscale; if float ms, round.
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

# --------------------------------------------------------------------------------------
# Parquet storage + backfill (chunked)
# --------------------------------------------------------------------------------------

def _as_int(x: Any) -> int:
    """Coerce pandas/numpy scalars (and friends) to built-in int.

    Pylance is conservative about `int(object)`; we narrow/cast explicitly.
    """
    # Fast paths
    if isinstance(x, int) and not isinstance(x, bool):
        return x
    if isinstance(x, (str, bytes, bytearray)):
        return int(x)

    # numpy/pandas scalar -> python scalar
    item = getattr(x, "item", None)
    if callable(item):
        try:
            v = item()
            # recurse once in case v is still a scalar
            if v is not x:
                return _as_int(v)
        except Exception:
            pass

    # Last resort: runtime conversion; cast for type checker.
    return int(cast("int | float | str | bytes | bytearray", x))

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _dt_series_to_epoch_ms(dt_utc: pd.Series) -> pd.Series:
    """
    dt_utc: Series[datetime64[ns, UTC]] (or anything pd.to_datetime can coerce)
    returns: Series[int64] epoch-ms, NaT -> 0
    """
    s = pd.to_datetime(dt_utc, utc=True, errors="coerce")
    ns = s.astype("int64")              # NaT -> int64 min
    ms = ns // 1_000_000
    ms = ms.where(ns != _I64_MIN, 0).astype("int64")
    return ms
def _to_epoch_ms_int(s: pd.Series) -> pd.Series:
    """Coerce datetime/int/float/object timestamps to epoch-ms int64."""
    if pd.api.types.is_datetime64_any_dtype(s):
        return (s.view("int64") // 1_000_000).astype("int64")

    if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
        dt = pd.to_datetime(s, utc=True, errors="coerce")
        if dt.notna().any():

            return _dt_series_to_epoch_ms(dt)

    x = pd.to_numeric(s, errors="coerce")
    # seconds vs ms heuristic: < 1e12 => seconds
    x = x.where(x.isna() | (x.abs() >= 1e12), x * 1000.0)
    return x.fillna(0).astype("int64")

def _interval_ms(interval: str) -> int:
    n = int("".join(ch for ch in interval if ch.isdigit()))
    u = "".join(ch for ch in interval if ch.isalpha())
    if u == "m":
        return n * 60_000
    if u == "h":
        return n * 3_600_000
    if u == "d":
        return n * 86_400_000
    raise ValueError(f"unsupported interval: {interval}")

def _normalize_ohlcv_df(df: pd.DataFrame, *, interval: str) -> pd.DataFrame:
    out = df.copy()
    step = _interval_ms(interval)

    out["open_time"] = _to_epoch_ms_int(out["open_time"]).astype("int64")
    out["data_ts"] = (out["open_time"] + step - 1).astype("int64")
    out["time"] = pd.to_datetime(out["data_ts"], unit="ms", utc=True)

    out = out.sort_values("data_ts", kind="stable").drop_duplicates(subset=["data_ts"], keep="last").reset_index(drop=True)
    out["_align_delta"] = (out["data_ts"] - out["open_time"]).astype("int64")  # should be step-1
    return out

def concat_chunks(chunks: Iterable[pd.DataFrame], *, dedup: bool = True) -> pd.DataFrame:
    """Utility: concat already-fetched chunks (keeps time semantics)."""
    parts = [c for c in chunks if c is not None and not c.empty]
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    if dedup and "data_ts" in df.columns:
        df = df.drop_duplicates(subset=["data_ts"], keep="last")
    if "data_ts" in df.columns:
        df = df.sort_values("data_ts", kind="stable").reset_index(drop=True)
    return df


@dataclass(frozen=True)
class BinanceOHLCVFetcherConfig:
    base_url: str = "https://api.binance.com"
    endpoint: str = "/api/v3/klines"
    timeout_s: float = 30.0
    max_limit: int = 1000  # Binance klines limit is up to 1000

@dataclass(frozen=True)
class BinanceOHLCVCleanerConfig:
    interval: str
    strict: bool = True
    dropna: bool = True
    drop_zero_price_bars: bool = True
    allow_empty: bool = True

@dataclass(frozen=True)
class BinanceOHLCVBackfillConfig:
    symbol: str
    interval: str
    start_ms: int | float | str | _dt.datetime | pd.Timestamp
    end_ms: int | float | str | _dt.datetime | pd.Timestamp

    limit: int | None = None

    # write switches
    write_raw: bool = True
    write_cleaned: bool = True

    # cleaner semantics
    strict: bool = False  # for historical backfill, default is non-strict (gaps are common)
    dropna: bool = True
    drop_zero_price_bars: bool = True

@dataclass(frozen=True)
class OHLCVParquetStoreConfig:
    """Filesystem layout for OHLCV parquet writes.
    Intended directory tree (per your design):
      <root>/<stage>/ohlcv/<symbol>/<interval>/<YYYY>.parquet
    """
    root: str = str(DATA_ROOT)
    domain: str = "ohlcv"

@dataclass(frozen=True)
class OHLCVCleanReport:
    n_in: int
    n_out: int
    dup_dropped: int
    rows_dropped_na: int
    glitch_bars_dropped: int
    zero_price_bars_dropped: int
    gaps: int
    gap_sizes_ms: list[int]
    first_data_ts: int | None
    last_data_ts: int | None

    def summary(self) -> str:
        lines = [
            "OHLCV Clean Report:",
            f"  Input rows: {self.n_in}",
            f"  Output rows: {self.n_out}",
            f"  Duplicates dropped: {self.dup_dropped}",
            f"  Rows dropped (NA): {self.rows_dropped_na}",
            f"  Glitch bars dropped: {self.glitch_bars_dropped}",
            f"  Zero-price bars dropped: {self.zero_price_bars_dropped}",
            f"  Gaps detected: {self.gaps}",
        ]
        if self.gaps > 0:
            lines.append(
                f"    Gap sizes (ms): {self.gap_sizes_ms[:10]}{'...' if len(self.gap_sizes_ms) > 10 else ''}"
            )
        lines.append(f"  First data_ts: {self.first_data_ts}")
        lines.append(f"  Last data_ts: {self.last_data_ts}")
        return "\n".join(lines)
    

class BinanceOHLCVFetcher:
    """Fetch Binance klines and normalize to v4 time semantics.

    Output schema (raw-normalized):
      - data_ts: int64 epoch ms (BAR CLOSE time)  <-- authoritative event time
      - open_time: int64 epoch ms (BAR OPEN time)
      - close_time: datetime64[ns, UTC] (inspection only)
      - core: open, high, low, close, volume
      - aux: quote_asset_volume, number_of_trades, taker_buy_*, ignore
    Notes:
      - This fetcher is intentionally *pure I/O + normalization*.
      - Cleaning (dedup rules, gap repair, resample, outliers) belongs to a separate cleaner.
    """

    def __init__(self, *, cfg: BinanceOHLCVFetcherConfig | None = None, session: requests.Session | None = None):
        self._cfg = cfg or BinanceOHLCVFetcherConfig()
        self._session = session or requests.Session()

    def _url(self) -> str:
        return f"{self._cfg.base_url}{self._cfg.endpoint}"

    def fetch_chunk(
        self,
        *,
        symbol: str,
        interval: str,
        start_ms: int | float | str | _dt.datetime | pd.Timestamp,
        end_ms: int | float | str | _dt.datetime | pd.Timestamp | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """Fetch a single kline chunk.

        start_ms/end_ms are coerced to epoch-ms ints.
        Returns an *empty* DataFrame if the API returns no rows.
        """
        start_ms_i = _coerce_epoch_ms(start_ms)
        end_ms_i = _coerce_epoch_ms(end_ms) if end_ms is not None else None

        lim = int(limit or self._cfg.max_limit)
        if lim <= 0 or lim > self._cfg.max_limit:
            raise ValueError(f"limit must be in [1, {self._cfg.max_limit}]")

        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ms_i,
            "limit": lim,
        }
        if end_ms_i is not None:
            params["endTime"] = end_ms_i

        r = self._session.get(self._url(), params=params, timeout=self._cfg.timeout_s)
        r.raise_for_status()
        payload = r.json()
        if not payload:
            return pd.DataFrame()

        cols = ["open_time","open","high","low","close","volume","close_time","quote_asset_volume","number_of_trades","taker_buy_base_asset_volume","taker_buy_quote_asset_volume","ignore",]
        df = pd.DataFrame(payload, columns=cols)

        # ---- enforce dtypes early (avoid CSV-era dtype drift) ----
        df["number_of_trades"] = df["number_of_trades"].astype("int64")

        for c in ("open","high","low","close","volume","quote_asset_volume","taker_buy_base_asset_volume",
                  "taker_buy_quote_asset_volume","ignore",):
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # ---- v4 time semantics ----
        step = _interval_ms(interval)
        df["close_time"] = df["close_time"].astype("int64")
        df["open_time"] = df["open_time"].astype("int64")
        df["close_time"] = df["close_time"].astype("int64")
        #### ------------
        # closetime may not be aligned -- binance glitch happens more on 2021; we recompute canonical data_ts
        #### ------------
        # canonical close (engine semantics)
        df["data_ts"] = (df["open_time"] + step - 1).astype("int64")
        df["time"] = pd.to_datetime(df["data_ts"], unit="ms", utc=True)
        # Order columns: time first.
        front = ["data_ts", "open_time", "time"]
        rest = [c for c in df.columns if c not in front]
        df = df[front + rest]

        return df

    def iter_range(
        self,
        *,
        symbol: str,
        interval: str,
        start_ms: int | float | str | _dt.datetime | pd.Timestamp,
        end_ms: int | float | str | _dt.datetime | pd.Timestamp,
        limit: int | None = None,
    ) -> Iterator[pd.DataFrame]:
        """Yield successive chunks covering [start_ms, end_ms) by advancing on open_time."""
        step = _interval_ms(interval)
        cur = _coerce_epoch_ms(start_ms)
        end = _coerce_epoch_ms(end_ms)

        while cur < end:
            df = self.fetch_chunk(symbol=symbol, interval=interval, start_ms=cur, end_ms=end, limit=limit)
            if df.empty:
                return
            yield df

            # Advance by last open_time + interval. Binance returns open_time aligned to interval.
            last_open = int(df["open_time"].iloc[-1])
            nxt = last_open + step
            if nxt <= cur:
                # Defensive: avoid infinite loop if payload is pathological.
                return
            cur = nxt

    def fetch_range(
        self,
        *,
        symbol: str,
        interval: str,
        start_ms: int | float | str | _dt.datetime | pd.Timestamp,
        end_ms: int | float | str | _dt.datetime | pd.Timestamp,
        limit: int | None = None,
        dedup: bool = True,
    ) -> pd.DataFrame:
        """Fetch a full range into one DataFrame.

        dedup=True will drop duplicates by `data_ts` and sort ascending.
        """
        parts: list[pd.DataFrame] = []
        for chunk in self.iter_range(symbol=symbol, interval=interval, start_ms=start_ms, end_ms=end_ms, limit=limit):
            parts.append(chunk)

        if not parts:
            return pd.DataFrame()

        df = pd.concat(parts, ignore_index=True)
        if dedup and "data_ts" in df.columns:
            df = df.drop_duplicates(subset=["data_ts"], keep="last")
        df = df.sort_values("data_ts", kind="stable").reset_index(drop=True)
        return df


class BinanceOHLCVCleaner:
    """Clean + validate normalized Binance OHLCV frames.

    Expected input columns (at minimum):
      - data_ts (int ms)
      - open_time (int ms)
      - open, high, low, close, volume

    Optional:
      - time (datetime-like; inspection only)
      - aux columns

    Guarantees on output:
      - sorted by data_ts ascending (stable)
      - duplicates dropped by data_ts (keep last)
      - data_ts/open_time are int64
      - numeric core columns are numeric (float64)
    """

    _CORE_NUM = ("open", "high", "low", "close", "volume")
    _REQ = ("data_ts", "open_time", *_CORE_NUM)

    def __init__(self, *, cfg: BinanceOHLCVCleanerConfig):
        self._cfg = cfg
        self._step = _interval_ms(cfg.interval)

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, OHLCVCleanReport]:
        if df is None or len(df) == 0:
            if self._cfg.allow_empty:
                rep = OHLCVCleanReport(
                    n_in=0,
                    n_out=0,
                    dup_dropped=0,
                    rows_dropped_na=0,
                    glitch_bars_dropped=0,
                    zero_price_bars_dropped=0,
                    gaps=0,
                    gap_sizes_ms=[],
                    first_data_ts=None,
                    last_data_ts=None,
                )
                return pd.DataFrame(), rep
            raise ValueError("Empty OHLCV frame")

        missing = [c for c in self._REQ if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        n_in = int(len(df))

        # ---- schema normalization FIRST (prevents parquet drift) ----
        x = _normalize_ohlcv_df(df, interval=self._cfg.interval)
        # ---- numeric coercion ----
        for c in self._CORE_NUM:
            x[c] = pd.to_numeric(x[c], errors="coerce")
        if "number_of_trades" in x.columns:
            x["number_of_trades"] = pd.to_numeric(x["number_of_trades"], errors="coerce")

        # ---- NA drop ----
        rows_dropped_na = 0
        if self._cfg.dropna:
            before = len(x)
            x = x.dropna(subset=["data_ts", "open_time", *self._CORE_NUM])
            rows_dropped_na = before - len(x)

        # ---- Binance maintenance/glitch bars (drop) ----
        glitch_dropped = 0
        if len(x) > 0:
            glitch_mask = pd.Series(False, index=x.index)
            # OHLC all <=0 is invalid; common in outages/maintenance.
            glitch_mask |= (x[["open", "high", "low", "close"]] <= 0).all(axis=1)
            # even stricter signature: OHLCV all == 0
            glitch_mask |= (x[["open", "high", "low", "close", "volume"]] == 0).all(axis=1)

            glitch_dropped = int(glitch_mask.sum())
            if glitch_dropped:
                x = x.loc[~glitch_mask].copy()

        # ---- zero-price bar drop (after glitch drop; do not double-count) ----
        zero_price_bars_dropped = 0
        if self._cfg.drop_zero_price_bars and len(x) > 0:
            before = len(x)
            is_zero_bar = (x[["open", "high", "low", "close"]] == 0).all(axis=1)
            x = x.loc[~is_zero_bar].copy()
            zero_price_bars_dropped = before - len(x)

        # ---- enforce int64 for timestamps ----
        try:
            x["data_ts"] = x["data_ts"].astype("int64")
            x["open_time"] = x["open_time"].astype("int64")
        except Exception as e:
            raise TypeError("data_ts/open_time must be coercible to int64") from e

        # ---- dedup + sort ----
        x = x.sort_values("data_ts", kind="stable")
        before = len(x)
        x = x.drop_duplicates(subset=["data_ts"], keep="last")
        dup_dropped = before - len(x)
        x = x.reset_index(drop=True)

        # ---- gap detection (close-time cadence) ----
        gap_sizes: list[int] = []
        gaps = 0
        if len(x) >= 2:
            dt = x["data_ts"].diff().iloc[1:]
            bad = dt[(dt.notna()) & (dt != self._step)]
            gaps = int(len(bad))
            gap_sizes = [int(v) for v in bad.to_list()]

        if self._cfg.strict:
            if gaps != 0:
                raise ValueError(f"Detected {gaps} cadence gaps (ms diffs != {self._step}): {gap_sizes[:10]}")
            if (x["high"] < x[["open", "close"]].max(axis=1)).any() or (x["low"] > x[["open", "close"]].min(axis=1)).any():
                raise ValueError("OHLC invariant violated: high/low inconsistent with open/close")
            if (x[["open", "high", "low", "close"]] <= 0).any().any():
                raise ValueError("Non-positive prices detected after cleaning")

        n_out = int(len(x))
        rep = OHLCVCleanReport(
            n_in=n_in,
            n_out=n_out,
            dup_dropped=int(dup_dropped),
            rows_dropped_na=int(rows_dropped_na),
            glitch_bars_dropped=int(glitch_dropped),
            zero_price_bars_dropped=int(zero_price_bars_dropped),
            gaps=int(gaps),
            gap_sizes_ms=gap_sizes,
            first_data_ts=int(x["data_ts"].iloc[0]) if n_out else None,
            last_data_ts=int(x["data_ts"].iloc[-1]) if n_out else None,
        )
        return x, rep

class OHLCVParquetStore:
    def __init__(self, *, cfg: OHLCVParquetStoreConfig | None = None):
        self._cfg = cfg or OHLCVParquetStoreConfig()

    def _base_dir(self, *, stage: Literal["raw", "cleaned"], symbol: str, interval: str) -> Path:
        return Path(self._cfg.root) / stage / self._cfg.domain / symbol / interval

    def _year_path(self, *, stage: Literal["raw", "cleaned"], symbol: str, interval: str, year: int) -> Path:
        return self._base_dir(stage=stage, symbol=symbol, interval=interval) / f"{year:04d}.parquet"

    def _merge_write_year(self, *, new_df: pd.DataFrame, path: Path, interval: str) -> None:
        _ensure_dir(path.parent)

        new_df = _normalize_ohlcv_df(new_df, interval=interval)
        if path.exists():
            old_df = pd.read_parquet(path)
            old_df = _normalize_ohlcv_df(old_df, interval=interval)
            merged = pd.concat([old_df, new_df], ignore_index=True)
        else:
            merged = new_df

        merged = merged.sort_values("data_ts", kind="stable").drop_duplicates(subset=["data_ts"], keep="last")

        if "_align_delta" in merged.columns:
            merged = merged.drop(columns=["_align_delta"])

        tmp = path.with_suffix(path.suffix + ".tmp")
        merged.to_parquet(tmp, index=False)
        os.replace(tmp, path)

    def write_chunk(
        self,
        df: pd.DataFrame,
        *,
        stage: Literal["raw", "cleaned"],
        symbol: str,
        interval: str,
    ) -> list[Path]:
        if df is None or df.empty:
            return []
        if "data_ts" not in df.columns:
            raise ValueError("write_chunk requires data_ts column")

        ts = pd.to_datetime(df["data_ts"], unit="ms", utc=True)
        x = df.copy()
        x["_year"] = ts.dt.year.astype("int32")
        written: list[Path] = []
        for y, sub in x.groupby("_year", sort=True):
            year = _as_int(y)
            out = self._year_path(stage=stage, symbol=symbol, interval=interval, year=year)
            sub2 = sub.drop(columns=["_year"], errors="ignore")
            self._merge_write_year(new_df=sub2, path=out, interval=interval)
            written.append(out)
        return written

class BinanceOHLCVBackfiller:
    """Chunked OHLCV backfill: fetch -> (optional clean) -> write parquet.

    - Fetcher owns cursor progression (open_time + interval) to avoid cleaned-induced drift.
    - Cleaner never fills gaps; it may drop invalid rows and reports drops.
    - Store writes in day/year layout with dedup on data_ts.
    """

    def __init__(
        self,
        *,
        fetcher: BinanceOHLCVFetcher | None = None,
        store: OHLCVParquetStore | None = None,
    ):
        self._fetcher = fetcher or BinanceOHLCVFetcher()
        self._store = store or OHLCVParquetStore()

    def run(self, *, cfg: BinanceOHLCVBackfillConfig, stop_event: threading.Event | None = None) -> dict[str, Any]:
        cleaner = BinanceOHLCVCleaner(
            cfg=BinanceOHLCVCleanerConfig(
                interval=cfg.interval,
                strict=cfg.strict,
                dropna=cfg.dropna,
                drop_zero_price_bars=cfg.drop_zero_price_bars,
                allow_empty=True,
            )
        )

        totals = {
            "chunks": 0,
            "raw_rows": 0,
            "clean_rows": 0,
            "rows_dropped_na": 0,
            "zero_price_bars_dropped": 0,
            "dup_dropped_in_cleaner": 0,
            "written_raw_files": 0,
            "written_cleaned_files": 0,
            "glitch_bars_dropped": 0,
        }

        for chunk in self._fetcher.iter_range(
            symbol=cfg.symbol,
            interval=cfg.interval,
            start_ms=cfg.start_ms,
            end_ms=cfg.end_ms,
            limit=cfg.limit,
        ):
            if stop_event is not None and stop_event.is_set():
                break

            totals["chunks"] += 1
            totals["raw_rows"] += int(len(chunk))

            if cfg.write_raw:
                written = self._store.write_chunk(chunk, stage="raw", symbol=cfg.symbol, interval=cfg.interval)
                totals["written_raw_files"] += len(written)

            if cfg.write_cleaned:
                cleaned, rep = cleaner.clean(chunk)
                totals["clean_rows"] += int(len(cleaned))
                totals["rows_dropped_na"] += int(rep.rows_dropped_na)
                totals["zero_price_bars_dropped"] += int(rep.zero_price_bars_dropped)
                totals["dup_dropped_in_cleaner"] += int(rep.dup_dropped)
                totals["glitch_bars_dropped"] += int(rep.glitch_bars_dropped)
                written = self._store.write_chunk(cleaned, stage="cleaned", symbol=cfg.symbol, interval=cfg.interval)
                totals["written_cleaned_files"] += len(written)

        return totals


def main() -> None:
    stop_event = threading.Event()
    def _handle_stop(signum, _frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    for symbol in ["ETHUSDT","DOGEUSDT","ADAUSDT","SOLUSDT","XTZUSDT"]:
    # for symbol in ["BTCUSDT"]:
        for interval in ["1d", "4h", "1h", "15m"]:
            if stop_event.is_set():
                return
            print(f"Backfilling {symbol}...{interval}")

            parser = argparse.ArgumentParser(description="OHLCV backfill (Binance)")
            parser.add_argument("--root", default=str(DATA_ROOT), help="data root")
            parser.add_argument("--symbol", default=symbol)
            parser.add_argument("--interval", default=interval)
            parser.add_argument("--start", default="2020-01-01 00:00:00+00:00")
            parser.add_argument("--end", default=None, help="default: now (UTC)")
            args = parser.parse_args()

            end_ts = pd.Timestamp.utcnow()   # already UTC-aware in recent pandas
            end = args.end or end_ts.strftime("%Y-%m-%d %H:%M:%S+00:00")

            root = resolve_under_root(DATA_ROOT, args.root, strip_prefix="data")
            store = OHLCVParquetStore(cfg=OHLCVParquetStoreConfig(
                root=str(root), domain="ohlcv"
            ))
            bf = BinanceOHLCVBackfiller(store=store)

            start_year = pd.Timestamp(args.start).year
            end_year = pd.Timestamp(end).year

            for y in tqdm(range(start_year, end_year + 1), desc=f"backfill {args.symbol} {args.interval}"):
                if stop_event.is_set():
                    return
                y0 = pd.Timestamp(f"{y}-01-01 00:00:00+00:00")
                y1 = pd.Timestamp(f"{y+1}-01-01 00:00:00+00:00")
                s = max(pd.Timestamp(args.start), y0)
                e = min(pd.Timestamp(end), y1)
                if s >= e:
                    continue

                bf.run(cfg=BinanceOHLCVBackfillConfig(
                    symbol=args.symbol,
                    interval=args.interval,
                    start_ms=s.strftime("%Y-%m-%d %H:%M:%S+00:00"),
                    end_ms=e.strftime("%Y-%m-%d %H:%M:%S+00:00"),
                    limit=1000,
                    write_raw=False,
                    write_cleaned=True,
                    strict=False,
                ), stop_event=stop_event)

if __name__ == "__main__":
    main()
