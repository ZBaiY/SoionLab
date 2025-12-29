from __future__ import annotations

from typing import Any, Mapping, Dict, List, Tuple

from ingestion.contracts.tick import IngestionTick, Domain, _coerce_epoch_ms
from ingestion.contracts.market import annotate_payload_market
from ingestion.contracts.normalize import Normalizer


class BinanceOrderbookNormalizer(Normalizer):
    """
    Normalize Binance orderbook payloads into IngestionTick.
    """

    symbol: str
    domain: Domain = "orderbook"
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
        Normalize a single orderbook payload into an IngestionTick.
        """

        # --- detect payload shape ---
        # WS: { "e": "depthUpdate", "E": ..., "s": "BTCUSDT", "U": ..., "u": ..., "b": [...], "a": [...] }
        # REST: { "lastUpdateId": ..., "bids": [...], "asks": [...] }

        if "b" in raw and "a" in raw:  # WebSocket depth update
            bids = raw["b"]
            asks = raw["a"]
            event_ts = _coerce_epoch_ms(raw.get("E"))
            sym = self.symbol or raw.get("s")
        elif "bids" in raw and "asks" in raw:  # REST snapshot
            bids = raw["bids"]
            asks = raw["asks"]
            ts_raw = raw.get("T") or raw.get("timestamp") or raw.get("E")
            if ts_raw is None:
                raise ValueError("REST orderbook snapshot missing timestamp")
            event_ts = _coerce_epoch_ms(ts_raw)
            sym = self.symbol
        else:
            raise ValueError("Unsupported orderbook payload format")

        if sym is None:
            raise ValueError("Symbol must be provided or present in raw payload")

        # --- normalize levels ---
        def _levels(rows) -> List[Tuple[float, float]]:
            out: List[Tuple[float, float]] = []
            for price, qty in rows:
                out.append((float(price), float(qty)))
            return out

        payload = {
            "bids": _levels(bids),
            "asks": _levels(asks),
        }

        # data_ts: arrival time approximated by event time
        data_ts = event_ts

        payload = annotate_payload_market(
            payload,
            symbol=self.symbol,
            venue=self.venue,
            asset_class=self.asset_class,
            currency=self.currency,
            event_ts=data_ts,
            calendar=self.calendar,
            session=self.session,
            timezone_name=self.timezone_name,
        )

        return IngestionTick(
            domain=self.domain,
            symbol=self.symbol,
            timestamp=event_ts,
            data_ts=data_ts,
            payload=payload,
        )
