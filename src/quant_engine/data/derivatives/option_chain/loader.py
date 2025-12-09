# quant_engine/data/derivatives/loader.py
import pandas as pd
from .option_contract import OptionContract, OptionType
from .option_chain import OptionChain
import numpy as np
from datetime import datetime

def _clean(value):
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    return value

def _normalize_expiry(exp):
    """
    Normalize expiry into ISO format YYYY-MM-DD.
    Supports: 20250131, 2025-01-31, 31JAN25, epoch seconds.
    """
    exp = str(exp)

    # ISO-like already
    try:
        dt = pd.to_datetime(exp, utc=False)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    # numeric YYYYMMDD
    if exp.isdigit() and len(exp) == 8:
        try:
            dt = datetime.strptime(exp, "%Y%m%d")
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    # Fallback: try very hard
    try:
        dt = pd.to_datetime(exp)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return exp  # last resort

def _compute_ttm(expiry_iso, ts):
    """
    Compute TTM in years.
    Both expiry and ts must be ISO-like.
    """
    try:
        t1 = pd.to_datetime(ts)
        t2 = pd.to_datetime(expiry_iso)
        diff = (t2 - t1).total_seconds()
        return max(diff, 0) / (365*24*3600)
    except Exception:
        return None

class OptionChainLoader:
    """
    Loads raw option chain data from CSV / Parquet / API.
    Produces OptionChain objects suitable for FeatureLayer or IV modeling layer.
    """

    def load_from_csv(self, path: str) -> dict:
        df = pd.read_csv(path)
        return self._group_by_expiry(df)

    def load_from_dataframe(self, df: pd.DataFrame) -> dict:
        return self._group_by_expiry(df)

    def _group_by_expiry(self, df: pd.DataFrame) -> dict:
        """
        Expect df to have columns:
        ['symbol', 'expiry', 'strike', 'type', 'bid', 'ask', 'last',
         'volume', 'oi', 'iv', 'delta', 'gamma', 'vega', 'theta']
        """
        chains = []
        for expiry, group in df.groupby("expiry"):
            expiry_iso = _normalize_expiry(expiry)
            symbol = group['symbol'].iloc[0]

            contracts = []
            for _, row in group.iterrows():
                # compute TTM if timestamp column exists
                ts = row.get("timestamp", None)
                ttm = None
                if ts is not None:
                    ttm = _compute_ttm(expiry_iso, ts)

                row_dict = {
                    "symbol": symbol,
                    "expiry": expiry_iso,
                    "strike": row['strike'],
                    "type": row['type'],
                    "bid": _clean(row.get('bid')),
                    "ask": _clean(row.get('ask')),
                    "last": _clean(row.get('last')),
                    "volume": _clean(row.get('volume')),
                    "oi": _clean(row.get('oi')),
                    "iv": _clean(row.get('iv')),
                    "delta": _clean(row.get('delta')),
                    "gamma": _clean(row.get('gamma')),
                    "vega": _clean(row.get('vega')),
                    "theta": _clean(row.get('theta')),
                    "ttm": ttm,
                    "timestamp": ts,
                }

                contracts.append(
                    OptionContract(
                        symbol=symbol,
                        expiry=expiry_iso,
                        strike=row_dict["strike"],
                        option_type=OptionType(row_dict["type"]),
                        bid=row_dict["bid"],
                        ask=row_dict["ask"],
                        last=row_dict["last"],
                        volume=row_dict["volume"],
                        open_interest=row_dict["oi"],
                        implied_vol=row_dict["iv"],
                        delta=row_dict["delta"],
                        gamma=row_dict["gamma"],
                        vega=row_dict["vega"],
                        theta=row_dict["theta"],
                    )
                )

            chains.append(OptionChain(symbol=symbol, expiry=expiry_iso, contracts=contracts))

        return {
            "chains": chains,
            "timestamp": df["timestamp"].max() if "timestamp" in df.columns else None,
        }