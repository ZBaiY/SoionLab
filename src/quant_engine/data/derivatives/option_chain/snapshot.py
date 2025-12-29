from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Sequence, Hashable

from quant_engine.data.contracts.snapshot import Snapshot
from quant_engine.utils.num import to_float


def to_ms_int(x: Any) -> int:
    """Coerce a timestamp-like value to epoch milliseconds as int."""
    return int(to_float(x))


@dataclass(frozen=True)
class OptionChainSnapshot(Snapshot):
    """
    Immutable option chain snapshot.

    Represents a full option chain aligned to engine clock `timestamp`,
    derived from observation time `data_ts`.
    """

    # --- common snapshot fields ---
    timestamp: int
    data_ts: int
    latency: int
    symbol: str
    domain: str
    schema_version: int

    # --- option chain payload ---
    records: List[Dict[str, Any]]

    # ---------- helpers ----------

    @staticmethod
    def _normalize_records(chain: Any) -> List[Dict[str, Any]]:
        """
        Normalize raw chain input into List[Dict[str, Any]].

        Accepts:
          - list[dict]
          - pandas DataFrame (duck-typed)
        """
        if chain is None:
            return []

        # pandas DataFrame (duck-typed)
        if hasattr(chain, "to_dict"):
            try:
                return list(chain.to_dict(orient="records"))
            except Exception:
                pass

        if isinstance(chain, list):
            return [dict(r) for r in chain if isinstance(r, Mapping)]

        return []

    # ---------- constructors ----------

    @classmethod
    def from_chain_aligned(
        cls,
        *,
        timestamp: int,
        data_ts: int,
        symbol: str,
        chain: Any,
        schema_version: int = 1,
    ) -> "OptionChainSnapshot":
        ts = to_ms_int(timestamp)
        dts = to_ms_int(data_ts)

        records = cls._normalize_records(chain)

        return cls(
            timestamp=ts,
            data_ts=dts,
            latency=ts - dts,
            symbol=symbol,
            domain="option_chain",
            schema_version=schema_version,
            records=records,
        )

    # ---- backward-compatible wrapper ----

    @classmethod
    def from_chain(
        cls,
        ts: int,
        chain: Any,
        symbol: str,
    ) -> "OptionChainSnapshot":
        return cls.from_chain_aligned(
            timestamp=ts,
            data_ts=ts,
            symbol=symbol,
            chain=chain,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "data_ts": self.data_ts,
            "latency": self.latency,
            "symbol": self.symbol,
            "domain": self.domain,
            "schema_version": self.schema_version,
            "records": self.records,
        }