from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Literal

Domain = Literal[
    "ohlcv",
    "orderbook",
    "trades",
    "option_chain",
    "option_trades",
    "iv_surface",
    "sentiment",
]


def base_asset_from_symbol(symbol: str) -> str:
    """Return base asset for USDT-quoted symbols (e.g., BTCUSDT -> BTC)."""
    suffix = "USDT"
    if symbol.endswith(suffix):
        return symbol[: -len(suffix)]
    return symbol


def symbol_from_base_asset(asset: str, quote: str = "USDT") -> str:
    """Return quote-appended symbol (e.g., BTC -> BTCUSDT)."""
    if asset.endswith(quote):
        return asset
    return f"{asset}{quote}"


def _to_utc_date(ts_ms: int) -> date:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return dt.date()


def _iter_days_utc(start_ts: int, end_ts: int) -> Iterable[date]:
    """Yield UTC dates covering [start_ts, end_ts)."""
    if end_ts <= start_ts:
        return
    d0 = _to_utc_date(start_ts)
    d1 = _to_utc_date(end_ts - 1)  # end exclusive
    cur = d0
    while cur <= d1:
        yield cur
        cur += timedelta(days=1)


def _iter_years_utc(start_ts: int, end_ts: int) -> Iterable[int]:
    """Yield UTC years covering [start_ts, end_ts)."""
    if end_ts <= start_ts:
        return
    y0 = _to_utc_date(start_ts).year
    y1 = _to_utc_date(end_ts - 1).year
    for y in range(y0, y1 + 1):
        yield y


def resolve_cleaned_paths(
    *,
    data_root: Path,  # parent of "cleaned/"
    domain: Domain,
    start_ts: int,
    end_ts: int,
    symbol: str | None = None,
    interval: str | None = None,
    asset: str | None = None,
    venue: str | None = None,
    provider: str | None = None,
) -> list[Path]:
    """
    Pure path resolver (no IO). All timestamps are epoch ms (UTC).

    Layouts (your current design):

      ohlcv/
        <SYMBOL>/<INTERVAL>/<YEAR>.parquet

      option_chain/
        <ASSET>/<INTERVAL>/<YEAR>/<YYYY_MM_DD>.parquet

      trades/
        <SYMBOL>/<YEAR>/<MM>/<DD>.parquet

      option_trades/
        <VENUE>/<ASSET>/<YEAR>/<YYYY_MM_DD>.parquet

      sentiment/
        <PROVIDER>/<YEAR>/<MM>/<DD>.jsonl
    """
    root = data_root / "cleaned"

    if end_ts <= start_ts:
        return []

    if domain == "ohlcv":
        if not symbol or not interval:
            raise ValueError("ohlcv requires symbol and interval")
        base = root / "ohlcv" / symbol / interval
        return [base / f"{y}.parquet" for y in _iter_years_utc(start_ts, end_ts)]

    if domain == "orderbook":
        if not symbol:
            raise ValueError("orderbook requires symbol")
        base = root / "orderbook" / symbol
        out: list[Path] = []
        for d in _iter_days_utc(start_ts, end_ts):
            out.append(base / f"snapshot_{d.year:04d}-{d.month:02d}-{d.day:02d}.parquet")
        return out

    if domain == "trades":
        if not symbol:
            raise ValueError("trades requires symbol")
        base = root / "trades" / symbol
        out: list[Path] = []
        for d in _iter_days_utc(start_ts, end_ts):
            out.append(base / f"{d.year}" / f"{d.month:02d}" / f"{d.day:02d}.parquet")
        return out

    if domain == "option_chain":
        if not asset or not interval:
            raise ValueError("option_chain requires asset and interval")
        base = root / "option_chain" / asset / interval
        out: list[Path] = []
        for d in _iter_days_utc(start_ts, end_ts):
            out.append(base / f"{d.year}" / f"{d.year:04d}_{d.month:02d}_{d.day:02d}.parquet")
        return out

    if domain == "option_trades":
        if not venue or not asset:
            raise ValueError("option_trades requires venue and asset")
        base = root / "option_trades" / venue / asset
        out: list[Path] = []
        for d in _iter_days_utc(start_ts, end_ts):
            out.append(base / f"{d.year}" / f"{d.year:04d}_{d.month:02d}_{d.day:02d}.parquet")
        return out

    if domain == "iv_surface":
        # Not shown; placeholder pattern (likely daily under year/)
        if not asset or not interval:
            raise ValueError("iv_surface requires asset and interval")
        base = root / "iv_surface" / asset / interval
        out: list[Path] = []
        for d in _iter_days_utc(start_ts, end_ts):
            out.append(base / f"{d.year}" / f"{d.year:04d}_{d.month:02d}_{d.day:02d}.parquet")
        return out

    if domain == "sentiment":
        if not provider:
            raise ValueError("sentiment requires provider (e.g. 'news', 'reddit')")
        base = root / "sentiment" / provider
        out: list[Path] = []
        for d in _iter_days_utc(start_ts, end_ts):
            out.append(base / f"{d.year}" / f"{d.month:02d}" / f"{d.day:02d}.jsonl")
        return out

    raise ValueError(f"unsupported domain: {domain!r}")
