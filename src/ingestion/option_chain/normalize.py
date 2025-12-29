from __future__ import annotations

from typing import Any, Mapping

from ingestion.contracts.tick import IngestionTick, Domain, normalize_tick
from ingestion.contracts.normalize import Normalizer


class GenericOptionChainNormalizer(Normalizer):
    """
    Generic option chain normalizer.
    """
    
    symbol: str
    domain: Domain = "option_chain"

    def __init__(self, symbol: str):
        self.symbol = symbol

    def normalize(
        self,
        *,
        raw: Mapping[str, Any],
    ) -> IngestionTick:
        """
        Normalize a raw option chain payload into an IngestionTick.
        Expected (minimal) raw fields:
            - timestamp / ts / snapshot_time   (seconds or ms)
            - symbol / underlying
        """

        # --- timestamp extraction (best-effort, ingestion-level only) ---
        if "timestamp" in raw:
            ts = raw["timestamp"]
        elif "snapshot_time" in raw:
            ts = raw["snapshot_time"]
        elif "ts" in raw:
            ts = raw["ts"]
        else:
            raise ValueError("Option chain payload missing timestamp field")

        return normalize_tick(
            timestamp=ts,
            domain=self.domain,
            symbol=self.symbol,
            payload=dict(raw),
            data_ts=None,
        )