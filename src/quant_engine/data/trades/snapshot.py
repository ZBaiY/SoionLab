from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Dict

from quant_engine.data.contracts.snapshot import Snapshot
from quant_engine.utils.num import to_float


def to_ms_int(x: Any) -> int:
    """Coerce a timestamp-like value to epoch milliseconds as int."""
    return int(to_float(x))


@dataclass(frozen=True)
class TradesSnapshot(Snapshot):
    """
    Immutable trades snapshot.

    Represents a single aggregated trade (or trade print),
    aligned to engine clock `timestamp`,
    derived from trade event time `data_ts`.
    """

    # --- common snapshot fields ---
    # timestamp: int
    data_ts: int
    # latency: int
    symbol: str
    domain: str
    schema_version: int

    # --- trade core payload ---
    price: float
    size: float          # traded base quantity
    side: str            # "buy" | "sell"
    trade_id: int | None

    # --- auxiliary fields ---
    aux: Mapping[str, Any]

    @classmethod
    def from_trade_aligned(
        cls,
        *,
        timestamp: int,
        trade: Mapping[str, Any],
        symbol: str,
        schema_version: int = 1,
    ) -> "TradesSnapshot":
        """
        Tolerant constructor from an aligned trade event.

        Required trade fields (minimal):
            - trade["timestamp"] or trade["ts"]
            - trade["price"]
            - trade["size"] or trade["qty"]

        Optional:
            - trade_id
            - side / direction
        """
        ts = to_ms_int(timestamp)

        # event-time (trade occurrence)
        if "timestamp" in trade:
            trade_ts = to_ms_int(trade["timestamp"])
        elif "ts" in trade:
            trade_ts = to_ms_int(trade["ts"])
        else:
            trade_ts = ts

        # normalize core semantics
        price = to_float(trade.get("price", 0.0))
        size = to_float(trade.get("size", trade.get("qty", 0.0)))

        side = trade.get("side") or trade.get("direction")
        if side is not None:
            side = str(side).lower()

        trade_id = trade.get("trade_id") or trade.get("id")

        core_keys = {
            "timestamp",
            "ts",
            "price",
            "size",
            "qty",
            "side",
            "direction",
            "trade_id",
            "id",
        }
        aux = {k: v for k, v in trade.items() if k not in core_keys}
        assert isinstance(side, str), "Trade side must be a string if present"
        return cls(
            # timestamp=ts,
            data_ts=trade_ts,
            # latency=ts - trade_ts,
            symbol=symbol,
            domain="trades",
            schema_version=schema_version,
            price=price,
            size=size,
            side=side,
            trade_id=int(trade_id) if trade_id is not None else None,
            aux=aux,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            # "timestamp": self.timestamp,
            "data_ts": self.data_ts,
            # "latency": self.latency,
            "symbol": self.symbol,
            "domain": self.domain,
            "schema_version": self.schema_version,
            "price": self.price,
            "size": self.size,
            "side": self.side,
            "trade_id": self.trade_id,
            "aux": self.aux,
        }

    def to_dict_col(self, columns: list[str]) -> Dict[str, Any]:
        """
        Return a dict suitable for constructing a single-row DataFrame
        with selected columns.
        """
        full_dict = self.to_dict()
        aux = full_dict.get("aux", {})
        ans: Dict[str, Any] = {}

        for col in columns:
            if col in full_dict:
                ans[col] = full_dict[col]
            elif col in aux:
                ans[col] = aux.get(col)
            else:
                raise KeyError(f"Column {col} not found in TradesSnapshot")

        return ans