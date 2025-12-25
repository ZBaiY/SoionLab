from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, List

from quant_engine.data.contracts.snapshot import Snapshot
from quant_engine.utils.num import to_float


def to_ms_int(x: Any) -> int:
    """Coerce a timestamp-like value to epoch milliseconds as int."""
    return int(to_float(x))


@dataclass(frozen=True)
class SentimentSnapshot(Snapshot):
    """
    Immutable sentiment snapshot.

    Represents a sentiment observation aligned to engine clock `timestamp`,
    derived from observation time `data_ts`.
    """

    # --- common snapshot fields ---
    timestamp: int
    data_ts: int
    latency: int
    symbol: str
    domain: str
    schema_version: int

    # --- sentiment payload ---
    model: str
    score: float
    embedding: List[float] | None
    meta: Dict[str, Any]

    @classmethod
    def from_payload_aligned(
        cls,
        *,
        timestamp: int,
        data_ts: int,
        symbol: str,
        model: str,
        score: float,
        embedding: List[float] | None,
        meta: Mapping[str, Any] | None = None,
        schema_version: int = 1,
    ) -> "SentimentSnapshot":
        """
        Canonical tolerant constructor for sentiment payloads.
        """
        ts = to_ms_int(timestamp)
        dts = to_ms_int(data_ts)

        return cls(
            timestamp=ts,
            data_ts=dts,
            latency=ts - dts,
            symbol=symbol,
            domain="sentiment",
            schema_version=schema_version,
            model=model,
            score=to_float(score),
            embedding=list(embedding) if embedding is not None else None,
            meta=dict(meta or {}),
        )

    # ---- backward-compatible wrapper ----

    @classmethod
    def from_payload(
        cls,
        *,
        engine_ts: int,
        obs_ts: int,
        symbol: str,
        model: str,
        score: float,
        embedding: List[float] | None,
        meta: Mapping[str, Any] | None = None,
    ) -> "SentimentSnapshot":
        return cls.from_payload_aligned(
            timestamp=engine_ts,
            data_ts=obs_ts,
            symbol=symbol,
            model=model,
            score=score,
            embedding=embedding,
            meta=meta,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "data_ts": self.data_ts,
            "latency": self.latency,
            "symbol": self.symbol,
            "domain": self.domain,
            "schema_version": self.schema_version,
            "model": self.model,
            "score": self.score,
            "embedding": self.embedding,
            "meta": self.meta,
        }
