from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Dict
import datetime as dt
import re

from quant_engine.utils.num import to_float
from quant_engine.data.contracts.snapshot import Snapshot, MarketSpec, ensure_market_spec, MarketInfo

def to_ms_int(x: Any) -> int:
    return int(to_float(x))

def to_int(x: Any) -> int:
    return int(to_float(x))

# Regex for Deribit option instrument_name: e.g. BTC-28JUN24-60000-C
_DERIBIT_OPT_RE = re.compile(
    r"^(?P<underlying>[A-Z]+)-(?P<date>\d{2}[A-Z]{3}\d{2})-(?P<strike>[0-9.]+)-(?P<type>[CP])$"
)

def parse_deribit_expiry_ymd(instrument_name: str) -> int:
    """Parse Deribit option instrument_name into an expiry date key (YYYYMMDD as int)."""
    m = _DERIBIT_OPT_RE.match(instrument_name)
    if not m:
        raise ValueError(f"Unrecognized Deribit instrument_name: {instrument_name}")
    expiry_dt = dt.datetime.strptime(m.group("date"), "%d%b%y").date()
    return int(expiry_dt.strftime("%Y%m%d"))


def parse_deribit_expiry_ts_ms(instrument_name: str) -> int:
    """Parse Deribit option instrument_name into an expiry timestamp (epoch ms, UTC).

    Deribit weekly/monthly futures and options settle/expire at 08:00 UTC of the expiry date.
    The instrument_name encodes only the date, not the exact timestamp; this function applies
    the Deribit convention deterministically.

    If you later maintain an instrument metadata table (instrument_name -> expiration_timestamp),
    prefer that value as the source of truth.
    """
    m = _DERIBIT_OPT_RE.match(instrument_name)
    if not m:
        raise ValueError(f"Unrecognized Deribit instrument_name: {instrument_name}")
    expiry_date = dt.datetime.strptime(m.group("date"), "%d%b%y").date()
    expiry_dt = dt.datetime(
        expiry_date.year,
        expiry_date.month,
        expiry_date.day,
        8, 0, 0, 0,
        tzinfo=dt.timezone.utc,
    )
    return int(expiry_dt.timestamp() * 1000)

@dataclass(frozen=True)
class OptionTradeEvent(Snapshot):
    source: str
    schema_version: int
    symbol: str
    market: MarketSpec
    domain: str

    data_ts: int

    instrument_name: str
    expiry_ymd: int
    expiry_ts: int
    direction: str
    price: float
    amount: float

    trade_id: str | int
    trade_seq: int
    tick_direction: int
    iv: float
    mark_price: float
    index_price: float
    contracts: float

    aux: Mapping[str, Any]

    @classmethod
    def from_deribit(
        cls,
        *,
        trade: Mapping[str, Any],
        symbol: str,
        market: MarketSpec | None = None,
        schema_version: int = 1,
    ) -> "OptionTradeEvent":
        core = {
            "trade_seq","trade_id","data_ts","tick_direction","price","mark_price","iv",
            "instrument_name","index_price","direction","contracts","amount",
        }
        aux = {k: v for k, v in trade.items() if k not in core}

        # trade_id：deribit 有时给 int，有时给 str；统一容忍
        tid = trade.get("trade_id")
        if isinstance(tid, (int, str)):
            trade_id = tid
        else:
            # 最保守：转 str
            trade_id = str(tid)
        return cls(
            source="DERIBIT",
            schema_version=schema_version,
            symbol=symbol,
            market=ensure_market_spec(market),
            domain="option_trades",
            data_ts=to_ms_int(trade["data_ts"]),
            instrument_name=str(trade["instrument_name"]),
            expiry_ymd=parse_deribit_expiry_ymd(str(trade["instrument_name"])),
            expiry_ts=parse_deribit_expiry_ts_ms(str(trade["instrument_name"])),
            direction=str(trade["direction"]),
            price=to_float(trade["price"]),
            amount=to_float(trade["amount"]),
            trade_id=trade_id,
            trade_seq=to_int(trade["trade_seq"]),
            tick_direction=to_int(trade["tick_direction"]),
            iv=to_float(trade["iv"]),
            mark_price=to_float(trade["mark_price"]),
            index_price=to_float(trade["index_price"]),
            contracts=to_float(trade["contracts"]),
            aux=aux,
        )

    def to_dict(self) -> Dict[str, Any]:
        assert isinstance(self.market, MarketInfo)
        return {
            "source": self.source,
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "market": self.market.to_dict(),
            "domain": self.domain,
            "data_ts": self.data_ts,
            "instrument_name": self.instrument_name,
            "expiry_ymd": self.expiry_ymd,
            "expiry_ts": self.expiry_ts,
            "direction": self.direction,
            "price": self.price,
            "amount": self.amount,
            "trade_id": self.trade_id,
            "trade_seq": self.trade_seq,
            "tick_direction": self.tick_direction,
            "iv": self.iv,
            "mark_price": self.mark_price,
            "index_price": self.index_price,
            "contracts": self.contracts,
            "aux": dict(self.aux),
        }
