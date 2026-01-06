from __future__ import annotations

from typing import Any, Mapping

import time

from ingestion.contracts.tick import Domain, IngestionTick, normalize_tick
from ingestion.contracts.market import annotate_payload_market
from ingestion.contracts.normalize import Normalizer


# Canonical Binance kline order (REST / WS)
_BINANCE_KLINE_SCHEMA = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]


# Binance WS kline keys -> canonical keys
# https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams
_WS_KEYMAP = {
    "t": "open_time",
    "T": "close_time",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "v": "volume",
    "q": "quote_asset_volume",
    "n": "number_of_trades",
    "V": "taker_buy_base_asset_volume",
    "Q": "taker_buy_quote_asset_volume",
    "B": "ignore",
    "x": "is_closed",
}


def _now_ms() -> int: # UTC time in epoch milliseconds
    return int(time.time() * 1000)

def _coerce_binance_kline_mapping(k: Mapping[str, Any]) -> dict[str, Any]:
    """Return a dict using canonical Binance kline field names.

    Accepts either:
      - already-canonical REST-like dict with keys in _BINANCE_KLINE_SCHEMA
      - WS kline dict with short keys (t/T/o/h/l/c/v/...)
    """
    if "open_time" in k and "close_time" in k:
        return dict(k)

    out: dict[str, Any] = {}
    for kk, vv in k.items():
        mapped = _WS_KEYMAP.get(kk)
        if mapped is None:
            continue
        out[mapped] = vv
    return out


class BinanceOHLCVNormalizer(Normalizer):
    """
    Normalize Binance OHLCV (kline) payloads into IngestionTick.

    Supported raw formats:
        - REST klines: list[list[Any]]
        - WS kline event: dict with embedded kline payload
    """
    symbol: str
    domain: Domain = "ohlcv"
    venue: str
    asset_class: str
    currency: str | None
    calendar: str | None
    session: str | None
    timezone_name: str | None

    def __init__(
        self,
        symbol: str,
        *,
        venue: str = "binance",
        asset_class: str = "crypto",
        currency: str | None = None,
        calendar: str | None = None,
        session: str | None = None,
        timezone_name: str | None = None,
    ):
        self.symbol = symbol
        self.venue = venue
        self.asset_class = asset_class
        self.currency = currency
        self.calendar = calendar
        self.session = session
        self.timezone_name = timezone_name
        
    def normalize(
        self,
        *,
        raw: Mapping[str, Any],
    ) -> IngestionTick:
        """
        Normalize a single OHLCV payload into an IngestionTick.
        """

        # --- extract kline payload ---
        if "k" in raw:  # WebSocket event
            kline = raw["k"]
        else:  # REST-style payload
            kline = raw

        # --- normalize schema ---
        if isinstance(kline, (list, tuple)):
            if len(kline) < len(_BINANCE_KLINE_SCHEMA):
                raise ValueError("Invalid Binance kline payload length")
            payload = dict(zip(_BINANCE_KLINE_SCHEMA, kline))
        elif isinstance(kline, dict):
            payload = _coerce_binance_kline_mapping(kline)
        else:
            raise TypeError("Unsupported kline payload type")

        # --- timestamps ---
        # IngestionTick convention:
        #   - data_ts   : event/logical time (epoch ms)
        #   - timestamp : arrival/observe time (epoch ms)
        # For OHLCV, event time should be the bar close time when available.
        close_time = payload.get("close_time")
        open_time = payload.get("open_time")
        data_ts = payload.get("data_ts")
        if data_ts is not None:
            event_ts = int(data_ts)
        else:
            assert open_time is not None, "Binance OHLCV missing open_time"
            event_ts = int(close_time) if close_time is not None else int(open_time)

        # Prefer WS event time if present; otherwise use wall-clock now.
        arrival_ts_any = raw.get("E")
        arrival_ts = int(arrival_ts_any) if arrival_ts_any is not None else _now_ms()

        # --- canonical OHLCV payload (keep full schema in payload) ---
        out_payload: dict[str, Any] = {
            # core
            "open": float(payload["open"]),
            "high": float(payload["high"]),
            "low": float(payload["low"]),
            "close": float(payload["close"]),
            "volume": float(payload["volume"]),
            # time metadata
            "open_time": int(open_time) if open_time is not None else None,
            "close_time": int(close_time) if close_time is not None else None,
        }

        # optional aux fields (preserve if present)
        for k in (
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
            "is_closed",
        ):
            if k in payload:
                out_payload[k] = payload[k]

        # --- filter obvious maintenance/glitch bars ---
        if _is_invalid_bar(out_payload):
            raise ValueError("Invalid OHLCV bar (maintenance/glitch)")

        out_payload = annotate_payload_market(
            out_payload,
            symbol=self.symbol,
            venue=self.venue,
            asset_class=self.asset_class,
            currency=self.currency,
            event_ts=event_ts,
            calendar=self.calendar,
            session=self.session,
            timezone_name=self.timezone_name,
        )

        return normalize_tick(
            timestamp=arrival_ts,
            data_ts=event_ts,
            domain=self.domain,
            symbol=self.symbol,
            payload=out_payload,
        )


def _is_invalid_bar(payload: Mapping[str, Any]) -> bool:
    values = [
        float(payload.get("open", 0.0)),
        float(payload.get("high", 0.0)),
        float(payload.get("low", 0.0)),
        float(payload.get("close", 0.0)),
        float(payload.get("volume", 0.0)),
    ]
    if all(v == 0.0 for v in values):
        return True
    return any(v <= 0.0 for v in values)
