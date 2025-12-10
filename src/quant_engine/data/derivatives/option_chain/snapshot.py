from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Union

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

        # Basic ATM IV extraction: closest-to-money option by |moneyness|
        try:
            sorted_chain = sorted(records, key=lambda x: abs(x.get("moneyness", 0.0)))
            atm = sorted_chain[0]
            atm_iv = float(atm.get("iv", 0.0))
        except Exception:
            atm_iv = 0.0

        # Simple skew: iv(call) - iv(put) approx if present
        try:
            calls = [o for o in records if o.get("type") == "call"]
            puts = [o for o in records if o.get("type") == "put"]
            call_iv = calls[0].get("iv") if calls else 0.0
            put_iv = puts[0].get("iv") if puts else 0.0
            assert call_iv is not None and put_iv is not None
            skew = float(call_iv) - float(put_iv)
        except Exception:
            skew = 0.0

        # Smile dictionary: strike → iv
        smile: Dict[str, float] = {}
        for opt in records:
            strike = opt.get("strike")
            iv = opt.get("iv")
            if strike is not None and iv is not None:
                smile[str(strike)] = float(iv)

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

        # ATM estimate (closest-to-money by |moneyness| if available)
        try:
            sorted_chain = sorted(records, key=lambda x: abs(x.get("moneyness", 0.0)))
            atm = sorted_chain[0]
            atm_iv = float(atm.get("iv", 0.0))
        except Exception:
            atm_iv = 0.0

        # Skew
        try:
            calls = [o for o in records if o.get("type") == "call"]
            puts = [o for o in records if o.get("type") == "put"]
            call_iv = calls[0].get("iv") if calls else 0.0
            put_iv = puts[0].get("iv") if puts else 0.0
            assert call_iv is not None and put_iv is not None
            skew = float(call_iv) - float(put_iv)
        except Exception:
            skew = 0.0

        # Smile
        smile: Dict[str, float] = {}
        for opt in records:
            strike = opt.get("strike")
            iv = opt.get("iv")
            if strike is not None and iv is not None:
                smile[str(strike)] = float(iv)

        return cls(
            timestamp=chain_timestamp,
            chain=df,
            atm_iv=atm_iv,
            skew=skew,
            smile=smile,
            latency=float(ts - chain_timestamp),
        )