from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Any

from quant_engine.data.contracts.snapshot import Snapshot, MarketSpec, ensure_market_spec, MarketInfo
from quant_engine.utils.num import to_float


def to_ms_int(x: Any) -> int:
    """Coerce a timestamp-like value to epoch milliseconds as int."""
    return int(to_float(x))


@dataclass(frozen=True)
class OrderbookSnapshot(Snapshot):
    """
    Immutable orderbook snapshot (L1 + optional aggregated L2).

    Represents orderbook state aligned to engine clock `timestamp`,
    derived from tick timestamp `data_ts`.
    """

    # --- common snapshot fields ---
    timestamp: int
    data_ts: int
    latency: int
    symbol: str
    market: MarketSpec
    domain: str
    schema_version: int

    # --- L1 ---
    best_bid: float
    best_bid_size: float
    best_ask: float
    best_ask_size: float

    # --- L2 aggregated ---
    bids: List[Dict[str, float]] = field(default_factory=list)
    asks: List[Dict[str, float]] = field(default_factory=list)

    # ---------- helpers ----------

    @staticmethod
    def _ensure_depth_list(x: Any) -> List[Dict[str, float]]:
        """
        Normalize bids/asks into List[Dict[str, float]].
        Acceptable input:
          - list of dicts {"price": ..., "qty": ...}
          - list of (price, qty)
          - None / invalid -> empty list
        """
        cleaned: List[Dict[str, float]] = []

        if isinstance(x, list):
            for item in x:
                if isinstance(item, dict):
                    cleaned.append(
                        {
                            "price": to_float(item.get("price", 0.0)),
                            "qty": to_float(item.get("qty", 0.0)),
                        }
                    )
                elif isinstance(item, (tuple, list)) and len(item) == 2:
                    price, qty = item
                    cleaned.append(
                        {
                            "price": to_float(price),
                            "qty": to_float(qty),
                        }
                    )
        return cleaned

    # ---------- constructors ----------

    @classmethod
    def from_tick_aligned(
        cls,
        *,
        timestamp: int,
        tick: Mapping[str, Any],
        symbol: str,
        market: MarketSpec | None = None,
        schema_version: int = 1,
    ) -> "OrderbookSnapshot":
        """
        Canonical tolerant constructor from an aligned orderbook tick.
        """
        ts = to_ms_int(timestamp)
        tick_ts = to_ms_int(tick.get("data_ts", tick.get("ts", ts)))
        return cls(
            timestamp=ts,
            data_ts=tick_ts,
            latency=ts - tick_ts,
            symbol=symbol,
            market=ensure_market_spec(market),
            domain="orderbook",
            schema_version=schema_version,
            best_bid=to_float(tick.get("best_bid", 0.0)),
            best_bid_size=to_float(tick.get("best_bid_size", 0.0)),
            best_ask=to_float(tick.get("best_ask", 0.0)),
            best_ask_size=to_float(tick.get("best_ask_size", 0.0)),
            bids=cls._ensure_depth_list(tick.get("bids")),
            asks=cls._ensure_depth_list(tick.get("asks")),
        )

    # ---- backward-compatible wrappers ----

    @classmethod
    def from_tick(
        cls,
        ts: int,
        tick: Mapping[str, Any],
        symbol: str,
        market: MarketSpec | None = None,
    ) -> "OrderbookSnapshot":
        return cls.from_tick_aligned(timestamp=ts, tick=tick, symbol=symbol, market=market)

    @classmethod
    def from_dataframe(
        cls,
        df: Any,
        ts: int,
        symbol: str,
        market: MarketSpec | None = None,
    ) -> "OrderbookSnapshot":
        """
        Backward-compatible helper for 1-row DataFrame input.
        Pandas is intentionally not imported at module scope.
        """
        row = df.iloc[0]
        tick = {
            "timestamp": int(row.get("data_ts", ts)),
            "ts": int(row.get("data_ts", ts)),
            "best_bid": row.get("best_bid", 0.0),
            "best_bid_size": row.get("best_bid_size", 0.0),
            "best_ask": row.get("best_ask", 0.0),
            "best_ask_size": row.get("best_ask_size", 0.0),
            "bids": row.get("bids", []),
            "asks": row.get("asks", []),
        }
        return cls.from_tick_aligned(timestamp=ts, tick=tick, symbol=symbol, market=market)

    # ---------- utilities ----------

    def mid_price(self) -> float:
        return 0.5 * (self.best_bid + self.best_ask)

    def spread(self) -> float:
        return self.best_ask - self.best_bid

    def to_dict(self) -> Dict[str, Any]:
        assert isinstance(self.market, MarketInfo)
        return {
            "timestamp": self.timestamp,
            "data_ts": self.data_ts,
            "latency": self.latency,
            "symbol": self.symbol,
            "market": self.market.to_dict(),
            "domain": self.domain,
            "schema_version": self.schema_version,
            "best_bid": self.best_bid,
            "best_bid_size": self.best_bid_size,
            "best_ask": self.best_ask,
            "best_ask_size": self.best_ask_size,
            "bids": self.bids,
            "asks": self.asks,
            "mid": self.mid_price(),
            "spread": self.spread(),
        }
