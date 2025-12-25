from __future__ import annotations
from typing import Any, Mapping
from ingestion.contracts.tick import IngestionTick, Domain, _coerce_epoch_ms
from ingestion.contracts.normalize import Normalizer


class GenericSentimentNormalizer(Normalizer):
    """
    Generic sentiment normalizer.
    """
    symbol: str
    domain: Domain = "sentiment"
    
    def __init__(self, symbol: str):
        self.symbol = symbol

    def normalize(
        self,
        *,
        raw: Mapping[str, Any],
    ) -> IngestionTick:
        """
        Normalize a raw sentiment payload into an IngestionTick.
        Expected (minimal) raw fields:
            - timestamp / published_at / ts  (seconds or ms)
        Optional:
            - symbol / asset / ticker
            - source / vendor / category
            - text / score / embedding ref
        The payload is passed through largely untouched.
        """

        # --- timestamp extraction (best-effort, ingestion-level only) ---
        if "timestamp" in raw:
            event_ts = _coerce_epoch_ms(raw["timestamp"])
        elif "published_at" in raw:
            event_ts = _coerce_epoch_ms(raw["published_at"])
        elif "ts" in raw:
            event_ts = _coerce_epoch_ms(raw["ts"])
        else:
            raise ValueError("Sentiment payload missing timestamp field")

        # --- symbol association (optional) ---
        symbol = (
            raw.get("symbol")
            or raw.get("asset")
            or raw.get("ticker")
            or "GLOBAL"
        )

        return IngestionTick(
            domain=self.domain,
            symbol=self.symbol,
            timestamp=event_ts,
            data_ts=event_ts,  # arrival time not guaranteed; default to event time
            payload=dict(raw),
        )