from dataclasses import dataclass, field
from typing import Dict, List
import pandas as pd

"""
{
  "timestamp": tick_ts,
  "bids": [(price, qty), ...],
  "asks": [(price, qty), ...],
  "mid": float,
  "spread": float,
  "latency": ts - tick_ts,
}
"""

@dataclass
class OrderbookSnapshot:
    """
    Lightweight container for L1/L2 orderbook snapshots.

    This can be expanded later to full depth,
    but for now we store:
        - best bid/ask (L1)
        - optional aggregated depth (L2)
        - timestamp
    """

    timestamp: float
    symbol: str

    # L1
    best_bid: float
    best_bid_size: float
    best_ask: float
    best_ask_size: float

    # L2 aggregated data
    bids: List[Dict[str, float]] = field(default_factory=list)
    asks: List[Dict[str, float]] = field(default_factory=list)

    latency: float = 0.0

    @staticmethod
    def _ensure_depth_list(x):
        """
        Normalize bids/asks into List[Dict[str, float]].
        Acceptable input forms:
            • list of dicts
            • list of (price, qty) tuples
            • stringified list (from CSV)
            • None / float / invalid → returns empty list
        """
        if isinstance(x, list):
            cleaned = []
            for item in x:
                if isinstance(item, dict):
                    # ensure proper float typing
                    cleaned.append({
                        "price": float(item.get("price", 0.0)),
                        "qty": float(item.get("qty", 0.0)),
                    })
                elif isinstance(item, (tuple, list)) and len(item) == 2:
                    price, qty = item
                    cleaned.append({
                        "price": float(price),
                        "qty": float(qty),
                    })
            return cleaned

        # Handle CSV string case
        if isinstance(x, str):
            try:
                parsed = eval(x)
                return OrderbookSnapshot._ensure_depth_list(parsed)
            except Exception:
                return []

        # Any other type → empty list
        return []

    def mid_price(self) -> float:
        """Return mid price if both bid and ask exist."""
        if self.best_bid is None or self.best_ask is None:
            return float("nan")
        return 0.5 * (self.best_bid + self.best_ask)

    def spread(self) -> float:
        """Return bid-ask spread."""
        if self.best_bid is None or self.best_ask is None:
            return float("nan")
        return float(self.best_ask - self.best_bid)

    def to_dict(self) -> Dict:
        """Convert to plain dict for logging or JSON."""
        return {
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "best_bid": self.best_bid,
            "best_bid_size": self.best_bid_size,
            "best_ask": self.best_ask,
            "best_ask_size": self.best_ask_size,
            "bids": self.bids,
            "asks": self.asks,
            "mid": self.mid_price(),
            "spread": self.spread(),
            "latency": self.latency,
        }

    @staticmethod
    def from_dataframe(df: pd.DataFrame, ts: float, symbol: str) -> "OrderbookSnapshot":
        """
        Convert 1-row DataFrame into snapshot.
        Expected columns:
            timestamp, best_bid, best_bid_size, best_ask, best_ask_size
        """
        row = df.iloc[0]
        latency = float(ts - row["timestamp"])

        return OrderbookSnapshot(
            timestamp=float(row["timestamp"]),
            symbol=symbol,
            best_bid=float(row["best_bid"]),
            best_bid_size=float(row["best_bid_size"]),
            best_ask=float(row["best_ask"]),
            best_ask_size=float(row["best_ask_size"]),
            bids=OrderbookSnapshot._ensure_depth_list(row.get("bids", [])),
            asks=OrderbookSnapshot._ensure_depth_list(row.get("asks", [])),
            latency=latency,
        )

    @classmethod
    def from_tick(cls, ts: float, tick: Dict[str, float], symbol: str):
        tick_ts = float(tick.get("timestamp", ts))
        return cls(
            timestamp=tick_ts,
            symbol=symbol,
            best_bid=float(tick.get("best_bid", 0.0)),
            best_bid_size=float(tick.get("best_bid_size", 0.0)),
            best_ask=float(tick.get("best_ask", 0.0)),
            best_ask_size=float(tick.get("best_ask_size", 0.0)),
            bids=OrderbookSnapshot._ensure_depth_list(tick.get("bids", [])),
            asks=OrderbookSnapshot._ensure_depth_list(tick.get("asks", [])),
            latency=float(ts - tick_ts),
        )
