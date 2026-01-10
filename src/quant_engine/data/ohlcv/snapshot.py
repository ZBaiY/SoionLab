from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Dict

from quant_engine.data.contracts.snapshot import Snapshot, MarketSpec, ensure_market_spec, MarketInfo
from quant_engine.utils.num import to_float


def to_ms_int(x: Any) -> int:
    """Coerce a timestamp-like value to epoch milliseconds as int."""
    return int(to_float(x))


@dataclass(frozen=True)
class OHLCVSnapshot(Snapshot):
    """
    Immutable OHLCV snapshot.

    Represents a single OHLCV bar aligned to engine clock `timestamp`,
    derived from bar close time `data_ts`.
    """

    # --- common snapshot fields ---
    # timestamp: int
    data_ts: int
    # latency: int
    symbol: str
    market: MarketSpec
    domain: str
    schema_version: int

    # --- OHLCV payload ---
    open: float
    high: float
    low: float
    close: float
    volume: float
    aux: Mapping[str, Any]

    @classmethod
    def from_bar_aligned(
        cls,
        *,
        timestamp: int,
        bar: Mapping[str, Any],
        symbol: str,
        market: MarketSpec | None = None,
        schema_version: int = 2,
    ) -> "OHLCVSnapshot":
        """
        Tolerant constructor from an aligned OHLCV bar.

        Required bar fields:
            - bar["timestamp"]
            - bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"]
        """
        ts = to_ms_int(timestamp)
        bar_ts = to_ms_int(bar.get("data_ts", ts)) if "data_ts" in bar else to_ms_int(bar.get("close_time", ts))

        core_keys = {"open", "high", "low", "close", "volume", "data_ts"}
        aux = {k: v for k, v in bar.items() if k not in core_keys}

        return cls(
            # timestamp=ts,
            data_ts=bar_ts,
            # latency=ts - bar_ts,
            symbol=symbol,
            market=ensure_market_spec(market),
            domain="ohlcv",
            schema_version=schema_version,
            open=to_float(bar.get("open", 0.0)),
            high=to_float(bar.get("high", 0.0)),
            low=to_float(bar.get("low", 0.0)),
            close=to_float(bar.get("close", 0.0)),
            volume=to_float(bar.get("volume", 0.0)),
            aux=aux,
        )

    def to_dict(self) -> Dict[str, Any]:
        assert isinstance(self.market, MarketInfo)
        return {
            # "timestamp": self.timestamp,
            "data_ts": self.data_ts,
            # "latency": self.latency,
            "symbol": self.symbol,
            "market": self.market.to_dict(),
            "domain": self.domain,
            "schema_version": self.schema_version,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "aux": self.aux,
        }
    
    def to_dict_col(self, columns: list[str]) -> Dict[str, Any]:
        """
        Return a dict suitable for constructing a single-row DataFrame with selected columns.
        """
        full_dict = self.to_dict()
        aux = full_dict.get("aux", {})
        ans = {}
        for col in columns:
            if col in full_dict:
                ans[col] = full_dict[col]
            elif col in aux: 
                ans[col] = aux.get(col)
            else:
                raise KeyError(f"Column {col} not found in OHLCVSnapshot")
        return ans
    
    def get_attr(self, key: str) -> Any:
        if not hasattr(self, key):
            raise AttributeError(f"{type(self).__name__} has no attribute {key!r}")
        return getattr(self, key)