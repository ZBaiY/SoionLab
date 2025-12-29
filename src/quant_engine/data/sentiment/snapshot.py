from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Dict

from quant_engine.data.contracts.snapshot import Snapshot, MarketSpec, ensure_market_spec, MarketInfo
from quant_engine.utils.num import to_float


def to_ms_int(x: Any) -> int:
    """Coerce a timestamp-like value to epoch milliseconds as int."""
    return int(to_float(x))


@dataclass(frozen=True)
class SentimentSnapshot(Snapshot):
    """
    Immutable sentiment snapshot.

    Represents a single sentiment-bearing document or message
    (news title, article, headline, etc.), aligned to engine clock `timestamp`,
    derived from content publication time `data_ts`.
    """

    # --- common snapshot fields ---
    # timestamp: int
    data_ts: int
    # latency: int
    symbol: str
    market: MarketSpec
    domain: str
    schema_version: int

    # --- sentiment core ---
    score: float | None    # raw sentiment score (e.g. VADER compound), optional at ingest
    source: str            # e.g. "news", "rss", "decrypt", "coindesk"

    # --- auxiliary payload ---
    aux: Mapping[str, Any]

    @classmethod
    def from_event_aligned(
        cls,
        *,
        timestamp: int,
        event: Mapping[str, Any],
        symbol: str,
        market: MarketSpec | None = None,
        schema_version: int = 1,
    ) -> "SentimentSnapshot":
        """
        Tolerant constructor from a sentiment-bearing event.

        Required event fields:
            - event["timestamp"] or event["published_ts"]
            - event["source"]

        Optional:
            - score (raw sentiment score, may be computed later)
            - text
            - title
            - url
            - author
        """
        ts = to_ms_int(timestamp)

        # publication / event time
        if "data_ts" in event:
            data_ts = to_ms_int(event["data_ts"])
        elif "published_ts" in event:
            data_ts = to_ms_int(event["published_ts"])
        else:
            data_ts = ts

        score = event.get("score")
        score = to_float(score) if score is not None else None
        source = str(event.get("source", "unknown"))

        core_keys = {
            "data_ts",
            "published_ts",
            "score",
            "source",
        }
        aux = {k: v for k, v in event.items() if k not in core_keys}

        return cls(
            # timestamp=ts,
            data_ts=data_ts,
            # latency=ts - data_ts,
            symbol=symbol,
            market=ensure_market_spec(market),
            domain="sentiment",
            schema_version=schema_version,
            score=score,
            source=source,
            aux=aux,
        )

    def to_dict(self) -> Dict[str, Any]:
        assert isinstance(self.market, MarketInfo)
        return {
            # "timestamp": self.timestamp,
            "data_ts": self.data_ts,
            # "latency": self.latency,
            "symbol": self.symbol,
            "market": self.market.to_dict(),
            "domain": self.domain,
            "schema_version": self.schema_version,
            "score": self.score,
            "source": self.source,
            "aux": self.aux,
        }

    def to_dict_col(self, columns: list[str]) -> Dict[str, Any]:
        """
        Return a dict suitable for constructing a single-row DataFrame
        with selected columns.
        """
        full_dict = self.to_dict()
        aux = full_dict.get("aux", {})
        ans: Dict[str, Any] = {}

        for col in columns:
            if col in full_dict:
                ans[col] = full_dict[col]
            elif col in aux:
                ans[col] = aux.get(col)
            else:
                raise KeyError(f"Column {col} not found in SentimentSnapshot")

        return ans
