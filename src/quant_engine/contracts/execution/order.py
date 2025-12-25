from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, Union


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"
    IOC = "IOC"
    FOK = "FOK"
    POST_ONLY = "POST_ONLY"
    REDUCE_ONLY = "REDUCE_ONLY"


@dataclass
class Order:
    symbol: str                      # trading symbol, e.g. "BTCUSDT"
    side: OrderSide                 # BUY / SELL
    qty: float
    order_type: OrderType           # MARKET / LIMIT / STOP / ...
    price: Optional[float]          # None for MARKET
    timestamp: Optional[int] = None  # engine/event timestamp (epoch ms)
    tag: str = ""                   # strategy tag

    # Future‑proof extension slot for STOP, FOK, IOC, trailing stop, iceberg, etc.
    extra: Dict[str, Union[float, str, bool]] = field(default_factory=dict)

    def to_dict(self):
        return {
            "symbol": self.symbol,
            "side": self.side.value,
            "qty": self.qty,
            "order_type": self.order_type.value,
            "price": self.price,
            "timestamp": None if self.timestamp is None else int(self.timestamp),
            "tag": self.tag,
            "extra": self.extra,
        }