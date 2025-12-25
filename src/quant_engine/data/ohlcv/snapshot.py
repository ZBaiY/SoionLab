from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Dict

from quant_engine.data.contracts.snapshot import Snapshot
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
    timestamp: int
    data_ts: int
    latency: int
    symbol: str
    domain: str
    schema_version: int

    # --- OHLCV payload ---
    open: float
    high: float
    low: float
    close: float
    volume: float

    @classmethod
    def from_bar_aligned(
        cls,
        *,
        timestamp: int,
        bar: Mapping[str, Any],
        symbol: str,
        schema_version: int = 1,
    ) -> "OHLCVSnapshot":
        """
        Tolerant constructor from an aligned OHLCV bar.

        Required bar fields:
            - bar["timestamp"]
            - bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"]
        """
        ts = to_ms_int(timestamp)
        bar_ts = to_ms_int(bar.get("timestamp", ts))

        return cls(
            timestamp=ts,
            data_ts=bar_ts,
            latency=ts - bar_ts,
            symbol=symbol,
            domain="ohlcv",
            schema_version=schema_version,
            open=to_float(bar.get("open", 0.0)),
            high=to_float(bar.get("high", 0.0)),
            low=to_float(bar.get("low", 0.0)),
            close=to_float(bar.get("close", 0.0)),
            volume=to_float(bar.get("volume", 0.0)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "data_ts": self.data_ts,
            "latency": self.latency,
            "symbol": self.symbol,
            "domain": self.domain,
            "schema_version": self.schema_version,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }