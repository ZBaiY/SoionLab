from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Literal

import requests
import datetime as _dt
from pathlib import Path

import pandas as pd


Order = Literal["asc", "desc"]

WWW = "https://www.deribit.com"
HIST = "https://history.deribit.com"


def _coerce_epoch_ms(x: Any) -> int:
    """Coerce seconds-or-ms epoch into epoch-ms int (ms-int)."""
    if x is None:
        raise ValueError("timestamp cannot be None")
    if isinstance(x, bool):
        raise ValueError("invalid timestamp type: bool")
    if isinstance(x, int):
        return x * 1000 if x < 10_000_000_000 else x
    if isinstance(x, float):
        v = x * 1000.0 if x < 10_000_000_000 else x
        return int(round(v))
    try:
        return _coerce_epoch_ms(float(x))
    except Exception as e:
        raise ValueError(f"invalid timestamp: {x!r}") from e


def _deribit_get(path: str, params: dict[str, Any] | None = None, *, host: str = HIST, timeout: int = 15) -> dict[str, Any]:
    url = host.rstrip("/") + "/api/v2" + path
    r = requests.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    if j.get("error"):
        raise RuntimeError(f"Deribit error: {j['error']}")
    return j["result"]


def _now_ms(*, host: str = WWW) -> int:
    # /public/get_time works on both WWW and HIST; keep WWW as default.
    res = _deribit_get("/public/get_time", host=host)
    # result is an int in ms
    assert isinstance(res, int | float | str)
    return int(res)


@dataclass(frozen=True)
class DeribitOptionTradesRESTSource:
    """Deribit option trades source using the history host.

    Endpoint:
      - /public/get_last_trades_by_currency_and_time

    Notes:
      - This source is *raw IO only*; it does not normalize field names.
      - To fetch expired instruments reliably, use host=HIST.
      - Output is the raw `trade` dicts from Deribit.
      - Polling cadence (if any) is managed by the ingestion worker, not here.

    Typical usage (historical window):
      src = DeribitOptionTradesRESTSource(currency="BTC", start_timestamp_ms=..., end_timestamp_ms=...)
      for trade in src: ...

    Typical usage (recent lookback):
      src = DeribitOptionTradesRESTSource(currency="BTC", lookback_ms=6*3600*1000)
    """

    currency: str
    kind: str = "option"
    host: str = HIST
    per: int = 1000
    max_pages: int = 200
    order: Order = "asc"

    start_timestamp_ms: int | None = None
    end_timestamp_ms: int | None = None
    lookback_ms: int | None = None

    def _resolve_window(self) -> tuple[int, int]:
        if self.start_timestamp_ms is not None and self.end_timestamp_ms is not None:
            start_ms = _coerce_epoch_ms(self.start_timestamp_ms)
            end_ms = _coerce_epoch_ms(self.end_timestamp_ms)
            return (start_ms, end_ms)

        # default to [now-lookback, now]
        end_ms = _now_ms(host=WWW)
        lb = int(self.lookback_ms) if self.lookback_ms is not None else 6 * 3600 * 1000
        start_ms = end_ms - int(lb)
        return (int(start_ms), int(end_ms))

    def iter_pages(self) -> Iterable[list[dict[str, Any]]]:
        """Yield pages (lists) of raw trades.

        Uses descending paging (Deribit supports `has_more` + `end_timestamp` cursor).
        """
        start_ms, end_ms = self._resolve_window()
        cursor_end = int(end_ms)

        for _ in range(int(self.max_pages)):
            res = _deribit_get(
                "/public/get_last_trades_by_currency_and_time",
                {
                    "currency": str(self.currency),
                    "kind": str(self.kind),
                    "start_timestamp": int(start_ms),
                    "end_timestamp": int(cursor_end),
                    "count": int(self.per),
                    "sorting": "desc",
                },
                host=self.host,
            )

            trades = res.get("trades", [])
            if not trades:
                break

            # trades are dicts already; do not mutate
            yield trades

            if not bool(res.get("has_more", False)):
                break

            # move window backward
            min_ts = min(int(t.get("timestamp", cursor_end)) for t in trades)
            cursor_end = min_ts - 1
            if cursor_end <= start_ms:
                break

    def iter_pages_range(self, *, start_ms: int, end_ms: int) -> Iterable[list[dict[str, Any]]]:
        """Yield pages for a specific window (start_ms, end_ms)."""
        cursor_end = int(end_ms)

        for _ in range(int(self.max_pages)):
            res = _deribit_get(
                "/public/get_last_trades_by_currency_and_time",
                {
                    "currency": str(self.currency),
                    "kind": str(self.kind),
                    "start_timestamp": int(start_ms),
                    "end_timestamp": int(cursor_end),
                    "count": int(self.per),
                    "sorting": "desc",
                },
                host=self.host,
            )

            trades = res.get("trades", [])
            if not trades:
                break

            yield trades

            if not bool(res.get("has_more", False)):
                break

            min_ts = min(int(t.get("timestamp", cursor_end)) for t in trades)
            cursor_end = min_ts - 1
            if cursor_end <= start_ms:
                break

    def fetch_all(self) -> list[dict[str, Any]]:
        """Fetch all raw trades within the resolved window."""
        out: list[dict[str, Any]] = []
        for page in self.iter_pages():
            out.extend(page)
        return out

    def backfill(self, *, start_ts: int, end_ts: int) -> list[dict[str, Any]]:
        start_ms = _coerce_epoch_ms(start_ts)
        end_ms = _coerce_epoch_ms(end_ts)
        out: list[dict[str, Any]] = []
        for page in self.iter_pages_range(start_ms=start_ms, end_ms=end_ms):
            out.extend(page)
        return out

    def __iter__(self):
        """Iterate trades in the requested order.

        Default is ascending by (timestamp, trade_seq) for deterministic event-time iteration.
        """
        trades = self.fetch_all()

        def _key(t: dict[str, Any]) -> tuple[int, int]:
            ts = int(t.get("timestamp", 0))
            seq = int(t.get("trade_seq", 0))
            return (ts, seq)

        trades.sort(key=_key, reverse=(self.order == "desc"))
        for t in trades:
            yield t



def _infer_date_from_path(p: Path) -> _dt.date | None:
    """Infer a YYYY-MM-DD date for partitioned parquet files.

    Supported:
      - trades_YYYY-MM-DD.parquet
      - DD.parquet (date inferred from parent YYYY/MM[/DD] folders)
      - Any nesting depth under the currency base dir.
    """
    name = p.name

    # 1) explicit full-date filename
    if name.startswith("trades_"):
        core = name[len("trades_") :]
        if core.endswith(".parquet"):
            core = core[: -len(".parquet")]
        try:
            return _dt.date.fromisoformat(core)
        except Exception:
            return None

    # 2) day-only filename: DD.parquet
    if name.endswith(".parquet"):
        core = name[: -len(".parquet")]
        if core.isdigit() and 1 <= len(core) <= 2:
            day = int(core)
            # infer year/month from parents (expect .../YYYY/MM[/DD]/<file>)
            parts = list(p.parts)
            year = month = None
            for i in range(len(parts) - 1, -1, -1):
                s = parts[i]
                if year is None and len(s) == 4 and s.isdigit():
                    year = int(s)
                    # month should be the next component (closer to filename)
                    if i + 1 < len(parts):
                        m = parts[i + 1]
                        if len(m) == 2 and m.isdigit():
                            month = int(m)
                    break
            if year is None or month is None:
                return None
            try:
                return _dt.date(year, month, day)
            except Exception:
                return None

    return None


@dataclass(frozen=True)
class DeribitOptionTradesParquetSource:
    """Local parquet-backed Deribit option trades source.

    Expected layout:
      <root>/option_trades/DERIBIT/<currency>/YYYY/MM/(DD/)?<file>

    Supported filenames:
      - trades_YYYY-MM-DD.parquet
      - DD.parquet

    Semantics:
      - IO-only: reads parquet and yields row dicts.
      - No normalization: keys are whatever you stored in parquet.
      - Deterministic iteration: sorts by (timestamp, trade_seq) within each file.

    Typical usage:
      src = DeribitOptionTradesParquetSource(root="data", currency="BTC", start_date="2025-01-01", end_date="2025-01-31")
      for trade in src: ...
    """

    root: str | Path
    currency: str
    exchange: str = "DERIBIT"
    order: Order = "asc"

    # date window (inclusive)
    start_date: str | _dt.date | None = None
    end_date: str | _dt.date | None = None

    # parquet read controls
    columns: list[str] | None = None

    def _base_dir(self) -> Path:
        r = Path(self.root)
        return (r / "option_trades" / self.exchange / str(self.currency)).resolve()

    def _coerce_date(self, x: str | _dt.date | None) -> _dt.date | None:
        if x is None:
            return None
        if isinstance(x, _dt.date):
            return x
        return _dt.date.fromisoformat(str(x))

    def iter_files(self) -> Iterable[Path]:
        """Yield parquet files in the inclusive [start_date, end_date] window."""
        base = self._base_dir()
        if not base.exists():
            return []

        s = self._coerce_date(self.start_date)
        e = self._coerce_date(self.end_date)

        files: list[tuple[_dt.date, Path]] = []
        for p in base.rglob("*.parquet"):
            d = _infer_date_from_path(p)
            if d is None:
                continue
            if s is not None and d < s:
                continue
            if e is not None and d > e:
                continue
            files.append((d, p))

        files.sort(key=lambda t: t[0])
        return [p for _, p in files]

    def _iter_df_rows(self, df: pd.DataFrame, *, descending: bool = False) -> Iterable[dict[str, Any]]:
        if df is None or df.empty:
            return

        # standardize expected numeric fields if present
        for k in ("timestamp", "trade_seq"):
            if k in df.columns:
                df[k] = pd.to_numeric(df[k], errors="coerce")

        if "timestamp" in df.columns:
            if "trade_seq" in df.columns:
                df = df.sort_values(["timestamp", "trade_seq"], kind="mergesort", ascending=not descending)
            else:
                df = df.sort_values(["timestamp"], kind="mergesort", ascending=not descending)

        # yield dicts (pure-python)
        for row in df.to_dict(orient="records"):
            # pandas types this as dict[Hashable, Any]; enforce `str` keys for our contract
            yield {str(k): v for k, v in row.items()}

    def __iter__(self):
        """Iterate trade dicts in the requested order.

        Ordering guarantees:
          - `order="asc"`: deterministic event-time order across the selected files.
          - `order="desc"`: deterministic reverse event-time order.

        Note: We stream file-by-file to avoid materializing the full dataset in memory.
        """
        fps = list(self.iter_files())
        if self.order == "desc":
            fps.reverse()

        for fp in fps:
            df = pd.read_parquet(fp, columns=self.columns)
            for row in self._iter_df_rows(df, descending=(self.order == "desc")):
                yield row
