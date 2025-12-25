from __future__ import annotations

from typing import Any, Dict, Mapping

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
            payload = dict(kline)
        else:
            raise TypeError("Unsupported kline payload type")

        # --- timestamps ---
        # Binance times are in milliseconds; runtime convention is epoch-ms int.
        event_ts = int(payload["open_time"])
        # Prefer WS event time if present; otherwise fall back to close_time/open_time.
        arrival_ts = int(raw.get("E", payload.get("close_time", payload["open_time"])))

        # --- canonical OHLCV payload ---
        ohlcv = {
            "open": float(payload["open"]),
            "high": float(payload["high"]),
            "low": float(payload["low"]),
            "close": float(payload["close"]),
            "volume": float(payload["volume"]),
        }

        return IngestionTick(
            domain=self.domain,
            symbol=self.symbol,
            timestamp=event_ts,
            data_ts=arrival_ts,
            payload=ohlcv,
        )
