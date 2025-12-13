from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Union, Mapping, Hashable

import pandas as pd

ChainInput = Union[pd.DataFrame, Sequence[Dict[str, Any]]]

@dataclass
class OptionChainSnapshot:
    timestamp: float
    chain: pd.DataFrame
    atm_iv: float
    skew: float
    smile: Dict[str, float]
    latency: float

    @staticmethod
    def _extract_iv(opt: Mapping[Hashable, Any]) -> float:
        """
        Extract implied volatility in a tolerant way.

        Supports both legacy "iv" field and the newer "implied_vol" field
        that comes from OptionContract.to_dict().
        """
        iv = opt.get("iv")
        if iv is None:
            iv = opt.get("implied_vol")
        if iv is None:
            return 0.0
        try:
            return float(iv)
        except Exception:
            return 0.0

    @staticmethod
    def _extract_type(opt: Mapping[Hashable, Any]) -> str | None:
        """
        Normalize option type to "call" / "put".

        Supports both legacy "type" field ("call"/"put") and the
        newer "option_type" field ("C"/"P" or "CALL"/"PUT").
        """
        t = opt.get("type")
        if isinstance(t, str):
            t_lower = t.lower()
            if t_lower in ("call", "put"):
                return t_lower

        opt_type = opt.get("option_type")
        if isinstance(opt_type, str):
            ot = opt_type.upper()
            if ot in ("C", "CALL"):
                return "call"
            if ot in ("P", "PUT"):
                return "put"

        return None

    @classmethod
    def _compute_metrics(cls, records: Sequence[Mapping[Hashable, Any]]) -> tuple[float, float, Dict[str, float]]:
        """
        Compute ATM IV, skew and smile from a list of option records.

        This function is tolerant to mixed schemas and will work with both
        legacy dict-style chains and OptionContract.to_dict() output.
        """
        # ATM estimate (closest-to-money by |moneyness| if available)
        try:
            sorted_chain = sorted(records, key=lambda x: abs(x.get("moneyness", 0.0)))
            atm = sorted_chain[0]
            atm_iv = cls._extract_iv(atm)
        except Exception:
            atm_iv = 0.0

        # Skew: call_iv - put_iv
        try:
            calls = [o for o in records if cls._extract_type(o) == "call"]
            puts = [o for o in records if cls._extract_type(o) == "put"]

            call_iv = cls._extract_iv(calls[0]) if calls else 0.0
            put_iv = cls._extract_iv(puts[0]) if puts else 0.0
            skew = float(call_iv) - float(put_iv)
        except Exception:
            skew = 0.0

        # Smile: strike → iv
        smile: Dict[str, float] = {}
        for opt in records:
            strike = opt.get("strike")
            if strike is None:
                continue
            iv = cls._extract_iv(opt)
            smile[str(strike)] = float(iv)

        return float(atm_iv), float(skew), smile

    @staticmethod
    def _to_dataframe(chain: ChainInput) -> pd.DataFrame:
        """Normalize raw chain input (list[dict] or DataFrame) into a DataFrame.

        This keeps the public constructors flexible while ensuring that
        OptionChainSnapshot.chain is always a pandas DataFrame.
        """
        if isinstance(chain, pd.DataFrame):
            return chain.copy()
        # Fallback: interpret as sequence of dict-like rows
        return pd.DataFrame(list(chain)) if chain else pd.DataFrame()

    @classmethod
    def from_chain(cls, timestamp: float, chain: ChainInput) -> "OptionChainSnapshot":
        """Construct a snapshot from raw chain data at a given timestamp.

        This is a non-aligned constructor (latency is set to 0.0). Use
        from_chain_aligned() when you have both engine ts and chain ts.
        """
        df = cls._to_dataframe(chain)

        if df.empty:
            return cls(timestamp, df, 0.0, 0.0, {}, 0.0)

        records = df.to_dict(orient="records")
        
        atm_iv, skew, smile = cls._compute_metrics(records)

        return cls(
            timestamp=timestamp,
            chain=df,
            atm_iv=atm_iv,
            skew=skew,
            smile=smile,
            latency=0.0,
        )

    @classmethod
    def from_chain_aligned(
        cls,
        ts: float,
        chain_timestamp: float,
        chain: ChainInput,
    ) -> "OptionChainSnapshot":
        """v4-aligned snapshot constructor.

        Parameters
        ----------
        ts : float
            Engine timestamp (current logical time).
        chain_timestamp : float
            Actual timestamp of the option chain observation.
        chain : ChainInput
            Raw chain data (list[dict] or DataFrame) to be normalized.
        """

        df = cls._to_dataframe(chain)

        if df.empty:
            return cls(
                timestamp=chain_timestamp,
                chain=df,
                atm_iv=0.0,
                skew=0.0,
                smile={},
                latency=float(ts - chain_timestamp),
            )

        records = df.to_dict(orient="records")
        atm_iv, skew, smile = cls._compute_metrics(records)

        return cls(
            timestamp=chain_timestamp,
            chain=df,
            atm_iv=atm_iv,
            skew=skew,
            smile=smile,
            latency=float(ts - chain_timestamp),
        )
    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to plain dict for logging or JSON serialization."""
        return {
            "timestamp": self.timestamp,
            "atm_iv": self.atm_iv,
            "skew": self.skew,
            "smile": self.smile,
            "latency": self.latency,
        }