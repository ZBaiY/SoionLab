from __future__ import annotations

from typing import Any, Mapping

import time

from ingestion.contracts.tick import IngestionTick, Domain
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


def _now_ms() -> int:
    return int(time.time() * 1000.0)


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
    def __init__(self, symbol: str):
        self.symbol = symbol
        
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

        return IngestionTick(
            domain=self.domain,
            symbol=self.symbol,
            timestamp=arrival_ts,
            data_ts=event_ts,
            payload=out_payload,
        )
