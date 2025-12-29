from __future__ import annotations

from typing import Protocol, Mapping, Any, runtime_checkable, Optional


@runtime_checkable
class MarketSpec(Protocol):
    """Market / instrument environment contract.

    This is the minimal set of market semantics needed to make data quality,
    sessions, and calendar-aware gap logic explicit (instead of crypto-only
    implicit assumptions).

    Notes:
      - Keep this small and stable; extend via optional fields or schema_versioned dicts.
      - For equities/futures/options, richer specs (corporate actions, rolls, chains)
        should live in dedicated domain contracts, referenced from snapshots.
    """

    venue: str                 # e.g. "binance", "nasdaq", "cme"
    asset_class: str           # e.g. "crypto", "equity", "future", "option"
    timezone: str              # IANA tz, e.g. "UTC", "America/New_York"
    calendar: str              # calendar identifier, e.g. "24x7", "XNYS"
    session: str               # session regime, e.g. "regular", "extended", "24x7"

    # Optional knobs (do NOT rely on these being present everywhere)
    currency: Optional[str]    # quote currency if known
    schema_version: int        # for tolerant evolution

    def to_dict(self) -> Mapping[str, Any]:
        ...


@runtime_checkable
class Snapshot(Protocol):
    """
    Immutable runtime snapshot contract.

    A Snapshot represents a frozen view of domain data at engine clock `timestamp` (epoch ms),
    derived from underlying data timestamp `data_ts` (epoch ms).

    Invariants:
    - timestamp >= data_ts (anti-lookahead)
    - latency == timestamp - data_ts (milliseconds)
    - to_dict() returns pure-python serializable objects
    - schema_version is used for tolerant evolution of snapshot fields
    - market encodes timezone/calendar/session semantics (no implicit 24/7 assumptions)
    """

    # --- timing ---
    # timestamp: int      # engine clock timestamp (epoch ms)
    symbol: str         # associated symbol
    market: MarketSpec  # market/session/calendar semantics for this snapshot

    data_ts: int        # data-origin timestamp (epoch ms)
    # latency: int        # timestamp - data_ts (ms)
    # --- identity ---
    domain: str        # "ohlcv" | "orderbook" | "option_chain" | "iv_surface" | ...
    schema_version: int  # for forward/backward compatibility

    def to_dict(self) -> Mapping[str, Any]:
        """
        Return a pure-python, serialization-safe representation.

        Must NOT return pandas / numpy objects.
        Must NOT expose internal mutable references.
        """
        ...