from dataclasses import dataclass
from enum import Enum
from typing import Any

class OptionType(str, Enum):
    CALL = "C"
    PUT = "P"

@dataclass
class OptionContract:
    symbol: str         # e.g. BTCUSDT
    expiry: str         # '2025-06-27'
    strike: float
    option_type: OptionType

    bid: float | None
    ask: float | None
    last: float | None
    volume: float | None
    open_interest: float | None

    implied_vol: float | None   # 如果上游已经提供
    delta: float | None
    gamma: float | None
    vega: float | None
    theta: float | None

    quote_ts: int | None = None  # quote timestamp (epoch ms), if upstream provides

    def mid(self) -> float | None:
        if self.bid is None or self.ask is None:
            return None
        return 0.5 * (float(self.bid) + float(self.ask))

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "expiry": self.expiry,
            "quote_ts": None if self.quote_ts is None else int(self.quote_ts),
            "strike": float(self.strike),
            "option_type": self.option_type.value,
            "bid": None if self.bid is None else float(self.bid),
            "ask": None if self.ask is None else float(self.ask),
            "last": None if self.last is None else float(self.last),
            "volume": None if self.volume is None else float(self.volume),
            "open_interest": None if self.open_interest is None else float(self.open_interest),
            "implied_vol": None if self.implied_vol is None else float(self.implied_vol),
            "delta": None if self.delta is None else float(self.delta),
            "gamma": None if self.gamma is None else float(self.gamma),
            "vega": None if self.vega is None else float(self.vega),
            "theta": None if self.theta is None else float(self.theta),
            "mid": self.mid(),
        }