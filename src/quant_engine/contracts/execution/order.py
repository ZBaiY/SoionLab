from dataclasses import dataclass
from typing import Optional


@dataclass
class Order:
    side: str                 # "BUY" or "SELL"
    qty: float
    order_type: str           # "MARKET" or "LIMIT"
    price: Optional[float]    # None for MARKET orders
    timestamp: Optional[float] = None
    tag: str = ""             # strategy tag