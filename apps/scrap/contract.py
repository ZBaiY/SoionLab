from __future__ import annotations
from typing import Protocol, Sequence, Mapping, Any
from dataclasses import dataclass

# -------------------------
# Global invariants
# -------------------------

# Venue / source identifiers (used in path layouts)
OHLCV_SOURCE: str = "BINANCE"
TRADES_SOURCE: str = "BINANCE"
OPTION_TRADES_SOURCE: str = "DERIBIT"
SENTIMENT_SOURCE: str = "NEWS"

# All event times are epoch milliseconds (ms-int)
TimestampMS = int

class StorageContract(Protocol):
    """
    Marker protocol.
    A storage contract defines:
        - path layout
        - required columns
        - column dtypes / semantics
    """

    @property
    def required_columns(self) -> Sequence[str]: ...

    def validate_schema(self, schema: Mapping[str, Any]) -> None:
        """
        Raise if schema is incompatible.
        """


# -------------------------
# OHLCV (klines)
# -------------------------

@dataclass(frozen=True)
class OHLCVContract(StorageContract):
    """OHLCV (klines) storage contract.

    Layout:
        data/klines/{source}/{symbol}/{interval}/{year}.parquet

    Timestamp convention (anti-lookahead):
      - `timestamp` is the bar CLOSE time in epoch-ms int.
      - `close_time` is a tz-aware datetime column for human inspection only.
      - runtime/engine MUST clock and align using `timestamp` only.

    Binance fields (raw): open_time(ms), close_time(ms), plus OHLCV + volumes.
    We keep `open_time` as epoch-ms int and store `close_time` as datetime.
    """

    symbol: str
    interval: str

    @property
    def required_columns(self) -> Sequence[str]:
        return (
            "timestamp",      # epoch ms int, bar CLOSE time (authoritative)
            "open_time",      # epoch ms int, bar OPEN time
            "close_time",     # tz-aware datetime, inspection only
            "open",
            "high",
            "low",
            "close",
            "volume",
        )

    @property
    def optional_columns(self) -> Sequence[str]:
        return (
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
        )

    def validate_schema(self, schema: Mapping[str, Any]) -> None:
        if "timestamp" not in schema:
            raise ValueError("OHLCV requires `timestamp` column")
        if "open_time" not in schema:
            raise ValueError("OHLCV requires `open_time` column")
        if "close_time" not in schema:
            raise ValueError("OHLCV requires `close_time` column")

        if not str(schema["timestamp"]).startswith("int"):
            raise TypeError("timestamp must be int64 epoch ms")
        if not str(schema["open_time"]).startswith("int"):
            raise TypeError("open_time must be int64 epoch ms")

        close_time_dtype = str(schema["close_time"])
        if close_time_dtype.startswith("int"):
            raise TypeError("close_time must NOT be integer-typed")
        if not ("datetime" in close_time_dtype or "Timestamp" in close_time_dtype):
            raise TypeError("close_time must be datetime-like (contain 'datetime' or 'Timestamp')")

        # Engine/runtime must NOT use close_time for clocking or alignment; use timestamp only.
        # NOTE: timestamp is CLOSE time by contract.


@dataclass(frozen=True)
class TradesContract(StorageContract):
    """Spot/perp executions (Binance aggTrades).

    Layout:
        data/trades/{source}/{symbol}/YYYY/MM/trades_YYYY-MM-DD.parquet

    Semantics:
      - One row = an aggregated trade (aggTrades)
      - `timestamp` is authoritative market execution time (epoch ms)
    """

    symbol: str

    @property
    def required_columns(self) -> Sequence[str]:
        return (
            "timestamp",        # int64 epoch ms, execution time (from T)
            "price",            # float, from p
            "quantity",         # float, from q
            "is_buyer_maker",   # bool, from m
            "agg_trade_id",     # int64, from a
        )

    @property
    def optional_columns(self) -> Sequence[str]:
        return (
            "first_trade_id",   # int64, from f
            "last_trade_id",    # int64, from l
        )


# -------------------------
# Sentiment (raw text events)
# -------------------------

@dataclass(frozen=True)
class SentimentRawContract(StorageContract):
    """Raw sentiment text events (no model score required).

    Layout:
        data/sentiment/{source}/{channel}/YYYY/MM/events_YYYY-MM-DD.parquet
        (or .jsonl if you prefer line-delimited text)

    Semantics:
      - One row = one text item (headline / snippet / post)
      - `timestamp` is publish time (or ingestion time if publish time unavailable)
      - score fields are OPTIONAL and model-specific
    """

    source: str = SENTIMENT_SOURCE
    channel: str = "news"  # e.g. news/rss, reddit, etc.

    @property
    def required_columns(self) -> Sequence[str]:
        return (
            "timestamp",  # epoch ms
            "text",
            "source",     # publisher / feed id
        )

    @property
    def optional_columns(self) -> Sequence[str]:
        return (
            "url",
            "title",
            "lang",
            "author",
            "score",      # generic scalar score (optional)
            "vader",      # e.g. compound (optional)
            "model",      # model name/version (optional)
        )


# -------------------------
# Orderbook snapshot
# -------------------------

@dataclass(frozen=True)
class OrderbookSnapshotContract(StorageContract):
    """
    For data/orderbook/{symbol}/snapshot_YYYY-MM-DD.parquet

    NOTE:
      Orderbook snapshots are collected as passive artifacts only.
      They are NOT part of the core data/feature pipeline (optional artifact).
    """

    symbol: str

    @property
    def required_columns(self) -> Sequence[str]:
        return (
            "timestamp",      # snapshot time, epoch ms
            "bids",           # list[(price, size)]
            "asks",           # list[(price, size)]
        )


# -------------------------
# Option chain
# -------------------------

@dataclass(frozen=True)
class OptionChainContract(StorageContract):
    """
    Option instrument metadata snapshot (NO prices).
    Source: DERIBIT get_instruments

    Layout (optional):
        data/option_instruments/{source}/{underlying}/instruments_YYYY-MM-DD.parquet

    Semantics:
      - One row = one listed option contract
      - Snapshot of existence, not trading activity
      - No guarantee of historical completeness

    This contract is used for discovery and bookkeeping only.
    It MUST NOT be consumed directly by runtime features.
    """

    underlying: str

    @property
    def required_columns(self) -> Sequence[str]:
        return (
            "timestamp",            # ingestion snapshot time (epoch ms)
            "instrument_name",      # unique option identifier
            "expiry_ts",            # epoch ms
            "strike",               # float
            "option_type",          # "C" / "P"
        )

    @property
    def optional_columns(self) -> Sequence[str]:
        return (
            "creation_timestamp",
            "is_active",
            "base_currency",
            "counter_currency",
        )

@dataclass(frozen=True)
class OptionTradesContract(StorageContract):
    """Event-level option execution data (raw fact source).

    Source: DERIBIT (history API)

    Layout:
        data/option_trades/{source}/{underlying}/YYYY/MM/trades_YYYY-MM-DD.parquet

    Semantics:
      - One row = one executed trade
      - `timestamp` is execution time (epoch ms)
      - Append-only, no aggregation
    """

    underlying: str

    @property
    def required_columns(self) -> Sequence[str]:
        return (
            "timestamp",          # execution time (epoch ms)
            "instrument_name",
            "price",              # trade price
            "amount",             # contracts traded
            "direction",          # buy / sell (aggressor)
            "trade_id",
        )

    @property
    def optional_columns(self) -> Sequence[str]:
        return (
            # Deribit-provided fields (may be null)
            "iv",
            "index_price",
            "mark_price",
            "tick_direction",
            "trade_seq",
            "contracts",
            "combo_trade_id",
            "combo_id",
            "block_trade_id",
            "block_trade_leg_count",
            # Derived/parsed fields (recommended for fast queries; can be reconstructed)
            "expiry_ts",
            "strike",
            "option_type",   # "C" / "P" (or "call"/"put" if you prefer; choose one consistently)
        )


# -------------------------
# IV surface
# -------------------------

@dataclass(frozen=True)
class IVSurfaceContract(StorageContract):
    """
    Runtime-derived implied volatility surface (NOT raw data).

    This contract is for diagnostics / research artifacts only.
    IV surfaces MUST be rebuildable from OptionTrades and
    must NOT be treated as historical ground truth.

    For artifacts/iv_surface/{source}/{underlying}/iv_YYYY-MM-DD.parquet

    IV surfaces are runtime states and may be partial, stale, or unavailable.
    """

    underlying: str

    @property
    def required_columns(self) -> Sequence[str]:
        return (
            "timestamp",      # surface evaluation time
            "expiry",         # epoch ms
            "moneyness",      # K / S or log-moneyness
            "iv",             # implied volatility
        )