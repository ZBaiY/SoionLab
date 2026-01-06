from __future__ import annotations

import asyncio
import datetime as _dt
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Iterable, Iterator, Mapping
import threading

import pandas as pd
import requests

from ingestion.contracts.source import Raw, Source

from quant_engine.utils.paths import data_root_from_file, resolve_under_root

DATA_ROOT = data_root_from_file(__file__, levels_up=3)


"""Raw payload keys expected (ingestion boundary):

Trades sources should yield dict-like rows that are *exchange-typed* but already
normalized to stable field names.

Canonical event-time:
    - data_ts: epoch milliseconds int (trade time)

Binance aggTrades (REST/WS) is the default implementation here.

Polling cadence (poll_time) is engineering-only:
  - It controls IO/fetch frequency.
  - It must NOT be treated as observation/semantic time.
"""

DEFAULT_POLL_INTERVAL_MS = 60_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000.0)


def _ymd_from_ms(ms: int) -> str:
    return _dt.datetime.fromtimestamp(ms / 1000.0, tz=_dt.timezone.utc).strftime("%Y-%m-%d")


def _coerce_ms(x: Any) -> int:
    """Coerce a timestamp-like value to epoch milliseconds int."""
    if x is None:
        raise TypeError("timestamp is None")
    if isinstance(x, (int, float)):
        v = float(x)
        # heuristic: seconds ~ 1e9, ms ~ 1e12
        if v < 10_000_000_000:
            v *= 1000.0
        return int(round(v))
    if isinstance(x, str):
        # numeric string
        try:
            return _coerce_ms(float(x))
        except Exception:
            pass
        # datetime string
        dt = pd.to_datetime(x, utc=True, errors="coerce")
        if pd.isna(dt):
            raise ValueError(f"Cannot parse timestamp: {x!r}")
        return int(dt.value // 1_000_000)
    if isinstance(x, (_dt.datetime, pd.Timestamp)):
        dt = pd.to_datetime(x, utc=True, errors="coerce")
        if pd.isna(dt):
            raise ValueError(f"Cannot parse timestamp: {x!r}")
        return int(dt.value // 1_000_000)
    raise TypeError(f"Unsupported timestamp type: {type(x)!r}")


def _binance_aggtrades_rest(
    *,
    symbol: str,
    limit: int = 1000,
    from_id: int | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    base_url: str = "https://api.binance.com",
    timeout: float = 10.0,
) -> list[dict[str, Any]]:
    """Fetch aggTrades from Binance REST.

    Endpoint: GET /api/v3/aggTrades

    Returned dicts are normalized to:
        - trade_id (aggTradeId)
        - price
        - quantity
        - first_trade_id
        - last_trade_id
        - data_ts (trade time ms)
        - is_buyer_maker
        - is_best_match
    """
    url = base_url.rstrip("/") + "/api/v3/aggTrades"
    params: dict[str, Any] = {
        "symbol": symbol,
        "limit": int(limit),
    }
    if from_id is not None:
        params["fromId"] = int(from_id)
    if start_time is not None:
        params["startTime"] = int(start_time)
    if end_time is not None:
        params["endTime"] = int(end_time)

    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected Binance aggTrades response type: {type(data)!r}")

    out: list[dict[str, Any]] = []
    for t in data:
        if not isinstance(t, dict):
            continue
        # Binance fields: a,p,q,f,l,T,m,M
        trade_id = t.get("a")
        trade_time = t.get("T")
        if trade_id is None or trade_time is None:
            continue
        f_f = t.get("f") if t.get("f") is not None else 0.
        l_f = t.get("l") if t.get("l") is not None else 0.0
        out.append(
            {
                "trade_id": int(trade_id),
                "price": t.get("p"),
                "quantity": t.get("q"),
                "first_trade_id": int(f_f) if f_f is not None else None,
                "last_trade_id": int(l_f) if l_f is not None else None,
                "data_ts": int(trade_time),
                "is_buyer_maker": bool(t.get("m")),
                "is_best_match": bool(t.get("M")),
                # preserve raw in case normalizer wants it
                "_raw": t,
            }
        )
    return out


def make_binance_aggtrades_fetch_fn(
    *,
    symbol: str,
    base_url: str = "https://api.binance.com",
    timeout: float = 10.0,
    limit: int = 1000,
    start_time: int | None = None,
    end_time: int | None = None,
) -> Callable[[], Iterable[Raw]]:
    """Factory returning a fetch_fn compatible with TradesRESTSource."""

    def _fetch() -> Iterable[Raw]:
        return _binance_aggtrades_rest(
            symbol=symbol,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
            base_url=base_url,
            timeout=timeout,
        )

    return _fetch


# ---------------------------------------------------------------------------
# Generic REST wrapper (sync)
# ---------------------------------------------------------------------------


class TradesRESTSource(Source):
    """Sync trades source that polls a `fetch_fn`.

    This is a generic wrapper used for REST adapters.
    It yields whatever rows `fetch_fn()` returns.

    IMPORTANT: rows should already include `data_ts` (epoch ms int).

    Poll interval is IO-only and must not be conflated with strategy intervals.
    """

    def __init__(
        self,
        *,
        fetch_fn: Callable[[], Iterable[Raw]],
        backfill_fn: Callable[[int, int], Iterable[Raw]] | None = None,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        stop_event: threading.Event | None = None,
    ):
        self._fetch_fn = fetch_fn
        self._backfill_fn = backfill_fn
        self._stop_event = stop_event

        if poll_interval_ms is not None:
            self._poll_interval_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            raise ValueError("One of poll_interval or poll_interval_ms must be provided")

        if self._poll_interval_ms <= 0:
            raise ValueError(f"poll interval must be > 0ms, got {self._poll_interval_ms}")

    def __iter__(self) -> Iterator[Raw]:
        while True:
            if self._stop_event is not None and self._stop_event.is_set():
                return
            for row in self._fetch_fn():
                yield row
            if self._sleep_or_stop(self._poll_interval_ms / 1000.0):
                return

    def _sleep_or_stop(self, seconds: float) -> bool:
        if self._stop_event is None:
            time.sleep(seconds)
            return False
        return self._stop_event.wait(seconds)

    def backfill(self, *, start_ts: int, end_ts: int) -> Iterable[Raw]:
        if self._backfill_fn is None:
            raise NotImplementedError("TradesRESTSource backfill requires backfill_fn")
        return self._backfill_fn(int(start_ts), int(end_ts))


# ---------------------------------------------------------------------------
# Binance concrete sources
# ---------------------------------------------------------------------------


class BinanceAggTradesRESTSource(Source):
    """Binance aggTrades source via REST polling.

    Emits only newly observed trades, strictly increasing by trade_id.
    Output rows include canonical `data_ts` (trade time) in ms-int.

    Notes:
      - This is meant for streaming / near-realtime.
      - For bulk backfills use your scraper (data/ writer) not this loop.
      - poll_interval is IO-only and does not affect runtime observation time.
    """

    def __init__(
        self,
        *,
        symbol: str,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        base_url: str = "https://api.binance.com",
        timeout: float = 10.0,
        limit: int = 1000,
        stop_event: threading.Event | None = None,
    ):
        self._symbol = symbol
        self._base_url = base_url
        self._timeout = float(timeout)
        self._limit = int(limit)
        self._stop_event = stop_event

        if poll_interval_ms is not None:
            self._poll_interval_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            # Default IO cadence (engineering-only).
            self._poll_interval_ms = DEFAULT_POLL_INTERVAL_MS

        if self._poll_interval_ms <= 0:
            raise ValueError(f"poll interval must be > 0ms, got {self._poll_interval_ms}")

        self._last_trade_id: int | None = None

    def __iter__(self) -> Iterator[Raw]:
        while True:
            if self._stop_event is not None and self._stop_event.is_set():
                return
            try:
                rows = _binance_aggtrades_rest(
                    symbol=self._symbol,
                    limit=self._limit,
                    from_id=(self._last_trade_id + 1) if self._last_trade_id is not None else None,
                    base_url=self._base_url,
                    timeout=self._timeout,
                )
            except Exception:
                if self._sleep_or_stop(self._poll_interval_ms / 1000.0):
                    return
                continue

            # emit in ascending trade_id order
            rows = sorted(rows, key=lambda r: int(r["trade_id"]))
            for r in rows:
                tid = int(r["trade_id"])
                if self._last_trade_id is None or tid > self._last_trade_id:
                    self._last_trade_id = tid
                    yield r

            if self._sleep_or_stop(self._poll_interval_ms / 1000.0):
                return

    def _sleep_or_stop(self, seconds: float) -> bool:
        if self._stop_event is None:
            time.sleep(seconds)
            return False
        return self._stop_event.wait(seconds)

    def backfill(self, *, start_ts: int, end_ts: int, limit: int = 1000) -> Iterable[Raw]:
        return _binance_aggtrades_rest(
            symbol=self._symbol,
            limit=int(limit),
            start_time=int(start_ts),
            end_time=int(end_ts),
            base_url=self._base_url,
            timeout=self._timeout,
        )


class BinanceAggTradesWebSocketSource(Source):
    """Binance aggTrade stream via WebSocket.

    Yields normalized rows with canonical `data_ts` (trade time, ms-int).
    """

    def __init__(
        self,
        *,
        symbol: str,
        base_url: str = "wss://stream.binance.com:9443/ws",
    ):
        self._symbol = symbol
        self._base_url = base_url.rstrip("/")

    async def __aiter__(self) -> AsyncIterator[Raw]:
        import json

        import websockets

        stream = f"{self._symbol.lower()}@aggTrade"
        url = f"{self._base_url}/{stream}"

        async with websockets.connect(url) as ws:
            async for msg in ws:
                try:
                    raw = json.loads(msg)
                except Exception:
                    continue
                if not isinstance(raw, dict):
                    continue
                # Binance WS aggTrade keys:
                #   e,E,s,a,p,q,f,l,T,m,M
                trade_id = raw.get("a")
                trade_time = raw.get("T")
                if trade_id is None or trade_time is None:
                    continue
                f_f = raw.get("f") if raw.get("f") is not None else 0.0
                l_f = raw.get("l") if raw.get("l") is not None else 0.0
                E_f = raw.get("E") if raw.get("E") is not None else 0.0
                yield {
                    "trade_id": int(trade_id),
                    "price": raw.get("p"),
                    "quantity": raw.get("q"),
                    "first_trade_id": int(f_f) if f_f is not None else None,
                    "last_trade_id": int(l_f) if l_f is not None else None,
                    "data_ts": int(trade_time),
                    "is_buyer_maker": bool(raw.get("m")),
                    "is_best_match": bool(raw.get("M")),
                    # retain WS event time if you want to treat it as observe time later
                    "event_ts": int(E_f) if E_f is not None else None,
                    "_raw": raw,
                }


# ---------------------------------------------------------------------------
# File source (daily parquet layout)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TradesFileLayout:
    """Trades parquet file layout, supporting both legacy and new formats:

    Legacy layout:
        root/
          └── <symbol>/
              ├── 2025-01-01.parquet
              ├── 2025-01-02.parquet
              └── ...

    New layout:
        root/
          └── <symbol>/
              └── YYYY/
                  └── MM/
                      ├── DD.parquet
                      ├── 2025-01-01.parquet
                      └── trades_2025-01-01.parquet

    The new layout allows daily files inside year/month folders, with flexible
    naming including DD.parquet, YYYY-MM-DD.parquet, or trades_YYYY-MM-DD.parquet.

    Parquet files must contain at least `data_ts` (epoch ms) or a fallback column.
    """

    root: Path

    def file_for(self, symbol: str, ymd: str) -> Path:
        yyyy, mm, dd = ymd.split("-")
        return self.root / symbol / yyyy / mm / f"{dd}.parquet"


def _parse_ymd_from_path(fp: Path) -> str | None:
    """Best-effort extract YYYY-MM-DD from a parquet file path."""
    stem = fp.stem

    # trades_YYYY-MM-DD.parquet or YYYY-MM-DD.parquet
    if stem.startswith("trades_"):
        stem2 = stem[len("trades_"):]
    else:
        stem2 = stem

    # full date in filename
    try:
        _dt.datetime.strptime(stem2, "%Y-%m-%d")
        return stem2
    except Exception:
        pass

    # DD.parquet with parent folders YYYY/MM
    try:
        dd = int(stem2)
        if 1 <= dd <= 31:
            mm = int(fp.parent.name)
            yyyy = int(fp.parent.parent.name)
            if 1 <= mm <= 12:
                return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
    except Exception:
        pass

    return None


def _iter_trade_files(sym_dir: Path) -> list[Path]:
    """Return all parquet files for a symbol directory, supporting legacy + new layouts."""
    files = [p for p in sym_dir.rglob("*.parquet") if p.is_file()]
    parsed: list[tuple[str, Path]] = []
    for p in files:
        ymd = _parse_ymd_from_path(p)
        if ymd is None:
            continue
        parsed.append((ymd, p))

    parsed.sort(key=lambda t: (t[0], str(t[1])))
    return [p for _, p in parsed]


class TradesFileSource(Source):
    """Trades source backed by local parquet files (daily layout)."""

    def __init__(
        self,
        *,
        root: str | Path,
        symbol: str,
        start_ms: int | None = None,
        end_ms: int | None = None,
        paths: Iterable[Path] | None = None,
        layout: TradesFileLayout | None = None,
    ):
        if layout is None:
            self._layout = TradesFileLayout(resolve_under_root(DATA_ROOT, root, strip_prefix="data"))
        else:
            base = resolve_under_root(DATA_ROOT, layout.root, strip_prefix="data")
            self._layout = TradesFileLayout(base)
        self._symbol = symbol
        self._start_ms = _coerce_ms(start_ms) if start_ms is not None else None
        self._end_ms = _coerce_ms(end_ms) if end_ms is not None else None
        if paths is not None:
            resolved_paths: list[Path] = []
            for p in paths:
                path = Path(p)
                if not path.is_absolute():
                    path = self._layout.root / path
                resolved_paths.append(path)
            self._paths: list[Path] | None = resolved_paths
        else:
            self._paths = None

    def __iter__(self) -> Iterator[Raw]:
        # resolve date range from ms bounds if provided; otherwise read all files
        if self._paths is not None:
            files = [p for p in self._paths if p.exists()]
        else:
            sym_dir = self._layout.root / self._symbol
            if not sym_dir.exists():
                return
            files = _iter_trade_files(sym_dir)
        if not files:
            return

        # If bounds exist, limit to covering dates
        if self._start_ms is not None:
            start_ymd = _ymd_from_ms(self._start_ms)
        else:
            start_ymd = None
        if self._end_ms is not None:
            end_ymd = _ymd_from_ms(self._end_ms)
        else:
            end_ymd = None

        for fp in files:
            ymd = _parse_ymd_from_path(fp)
            if ymd is None:
                continue
            if start_ymd is not None and ymd < start_ymd:
                continue
            if end_ymd is not None and ymd > end_ymd:
                continue

            df = pd.read_parquet(fp)
            if df is None or df.empty:
                continue

            # Normalize event-time field name
            if "data_ts" not in df.columns:
                if "timestamp" in df.columns:
                    df["data_ts"] = df["timestamp"].map(_coerce_ms)
                elif "T" in df.columns:
                    # raw Binance WS aggTrade
                    df["data_ts"] = df["T"].map(_coerce_ms)
                elif "trade_time" in df.columns:
                    df["data_ts"] = df["trade_time"].map(_coerce_ms)
                else:
                    raise ValueError(f"Parquet file {fp} missing 'data_ts' and no fallback timestamp column")

            df = df.dropna(subset=["data_ts"]).copy()
            df["data_ts"] = df["data_ts"].astype("int64", copy=False)
            df = df.sort_values("data_ts", kind="mergesort")

            if self._start_ms is not None:
                df = df[df["data_ts"] >= int(self._start_ms)]
            if self._end_ms is not None:
                df = df[df["data_ts"] <= int(self._end_ms)]

            # ensure we don't leak legacy field name
            if "timestamp" in df.columns:
                df = df.drop(columns=["timestamp"])

            for rec in df.to_dict(orient="records"):
                out_rec = {str(k): v for k, v in rec.items()}
                if "timestamp" in out_rec and "data_ts" not in out_rec:
                    out_rec["data_ts"] = _coerce_ms(out_rec.pop("timestamp"))
                elif "timestamp" in out_rec:
                    out_rec.pop("timestamp", None)
                yield out_rec
