from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict

"""
{
  "timestamp": bar_ts,
  "open": ...,
  "high": ...,
  "low": ...,
  "close": ...,
  "volume": ...,
  "latency": ts - bar_ts,
}
"""

@dataclass
class OHLCVSnapshot:
    """
    Unified OHLCV snapshot returned by OHLCV handlers via get_snapshot(ts).

    All handlers must guarantee:
        - timestamp: the bar's close time
        - values are floats
        - no look-ahead (the chosen bar must satisfy bar_ts <= ts)
        - latency = ts - bar_ts
    """
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    latency: float

    @classmethod
    def from_bar(cls, ts: float, bar: Dict[str, Any]) -> "OHLCVSnapshot":
        """
        Construct a snapshot from a bar dictionary.

        Required bar fields:
            bar["timestamp"], bar["open"], bar["high"],
            bar["low"], bar["close"], bar["volume"]
        """
        bar_ts = float(bar.get("timestamp", ts))

        return cls(
            timestamp=bar_ts,
            open=float(bar.get("open", 0.0)),
            high=float(bar.get("high", 0.0)),
            low=float(bar.get("low", 0.0)),
            close=float(bar.get("close", 0.0)),
            volume=float(bar.get("volume", 0.0)),
            latency=float(ts - bar_ts),
        )
    def to_dict(self) -> Dict:
        """Convert to plain dict for logging or JSON."""
        return {
            "timestamp": self.timestamp,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "latency": self.latency,
        }