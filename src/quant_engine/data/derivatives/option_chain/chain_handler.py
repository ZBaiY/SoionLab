from __future__ import annotations
from typing import Dict, List, Optional
from dataclasses import dataclass, field
import pandas as pd

from quant_engine.data.derivatives.option_chain.option_chain import OptionChain
from quant_engine.data.derivatives.option_chain.option_contract import OptionContract, OptionType
from quant_engine.utils.logger import get_logger, log_debug, log_info
from quant_engine.data.derivatives.iv.surface import IVSurface
from quant_engine.data.derivatives.iv.snapshot import IVSurfaceSnapshot
from quant_engine.data.derivatives.option_chain.snapshot import OptionChainSnapshot


class OptionChainDataHandler:
    """
    Unified handler for managing option chains in:
        - Backtesting (loaded from CSV/Parquet)
        - Live Trading (incrementally updated via exchange feed)

    Responsibilities:
        • maintain multiple expiries
        • update contracts incrementally
        • provide latest chain snapshot for feature/IV layers
        • remove expired contracts
    """

    def __init__(self, symbol: str):
        self._logger = get_logger(self.__class__.__name__)
        self.symbol = symbol
        self.chains: Dict[str, OptionChain] = {}   # expiry -> OptionChain

    # ----------------------------------------------------------------------
    # 1. Loading initial chain (e.g., backtest starting state)
    # ----------------------------------------------------------------------
    def load_initial(self, data):
        """
        Load initial option chains.
        Accepts either:
            • List[OptionChain]       (v3 style)
            • {"chains": [...]}       (v4 loader snapshot)
        """
        # v4 snapshot format
        if isinstance(data, dict) and "chains" in data:
            chains = data["chains"]
        else:
            # assume old list format
            chains = data

        for i, chain in enumerate(chains):
            if i == 0:
                self.symbol = chain.symbol
            log_info(self._logger, "Loaded initial option chain", expiry=chain.expiry)
            self.chains[chain.expiry] = chain

    # ----------------------------------------------------------------------
    # 2. Full snapshot update (live API update)
    # ----------------------------------------------------------------------
        # ----------------------------------------------------------------------
    # Classmethod: build a real-time handler from historical loader
    # ----------------------------------------------------------------------
    @classmethod
    def from_historical(cls, historical_handler):
        """
        Construct a live-style OptionChainDataHandler using the initial
        historical snapshot provided by HistoricalOptionChainHandler.

        Expected:
            historical_handler.load() has already been called, and
            historical_handler.data is in the format:
                {"chains": [OptionChain, OptionChain, ...], "timestamp": ...}
            OR directly:
                [OptionChain, OptionChain, ...]

        Returns:
            OptionChainDataHandler ready for real-time incremental updates.
        """
        snapshot = historical_handler.data

        if snapshot is None:
            raise ValueError("HistoricalOptionChainHandler has no loaded data. "
                             "Call historical_handler.load() first.")

        # Determine symbol from first chain
        first_chain = None
        if isinstance(snapshot, dict) and "chains" in snapshot:
            first_chain = snapshot["chains"][0]
        else:
            first_chain = snapshot[0]
        obj = cls(symbol=first_chain.symbol)

        # v4 format: {"chains": [...]}
        if isinstance(snapshot, dict) and "chains" in snapshot:
            chains = snapshot["chains"]
        else:
            # assume list of OptionChain
            chains = snapshot

        for chain in chains:
            obj.chains[chain.expiry] = chain

        return obj

    def on_new_snapshot(self, df: pd.DataFrame):
        """
        Receive a complete option-chain snapshot from exchange.

        Expected df columns:
        ['symbol','expiry','strike','type','bid','ask','last','volume','oi','iv','delta','gamma','vega','theta']
        """
        log_debug(self._logger, "Received new full option chain snapshot", rows=len(df))

        expiries = df['expiry'].unique()
        for expiry in expiries:
            sub = df[df.expiry == expiry]
            self.chains[expiry] = self._df_to_chain(sub)

    # ----------------------------------------------------------------------
    # 3. Incremental contract update (live tick updates)
    # ----------------------------------------------------------------------
    def update_contract(
        self,
        expiry: str,
        strike: float,
        option_type: OptionType,
        **fields,
    ):
        """
        Update a specific contract (bid/ask/iv/greeks/etc).
        Useful for high-frequency options data streams.
        """
        chain = self.chains.get(expiry)
        if chain is None:
            # Auto-create new expiry if needed
            chain = OptionChain(symbol=self.symbol, expiry=expiry)
            self.chains[expiry] = chain

        contract = chain.get_contract(strike, option_type)
        if contract:
            for k, v in fields.items():
                if hasattr(contract, k):
                    setattr(contract, k, v)
        else:
            # if contract not found, create it
            new_c = OptionContract(
                symbol=chain.symbol,
                expiry=expiry,
                strike=strike,
                option_type=option_type,
                bid=fields.get("bid"),
                ask=fields.get("ask"),
                last=fields.get("last"),
                volume=fields.get("volume"),
                open_interest=fields.get("open_interest") or fields.get("oi"),
                implied_vol=fields.get("iv") or fields.get("implied_vol"),
                delta=fields.get("delta"),
                gamma=fields.get("gamma"),
                vega=fields.get("vega"),
                theta=fields.get("theta"),
            )
            chain.contracts.append(new_c)

    # ----------------------------------------------------------------------
    # 4. Retrieval API
    # ----------------------------------------------------------------------
    def latest_chain(self) -> pd.DataFrame:
        """
        Return the combined latest chain snapshot across all expiries.
        """
        return self.get_latest_snapshot()

    def get_chain(self, expiry: str) -> Optional[OptionChain]:
        """
        Return OptionChain for a specific expiry.
        """
        return self.chains.get(expiry)

    def get_all_expiries(self) -> List[str]:
        return sorted(self.chains.keys())

    # ----------------------------------------------------------------------
    # 4b. Unified full snapshot of all expiries
    # ----------------------------------------------------------------------
    def get_latest_snapshot(self) -> pd.DataFrame:
        """
        Return a combined snapshot of all expiries as a single DataFrame.
        Useful for IV features, reporting, debugging.
        """
        frames = []
        for expiry, chain in self.chains.items():
            df = chain.to_dataframe()
            frames.append(df)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ----------------------------------------------------------------------
    # 4c. Snapshot for a specific expiry
    # ----------------------------------------------------------------------
    # ------------------------------------------------------------------
    # v4 timestamp-aligned snapshot retrieval
    # ------------------------------------------------------------------
    def get_snapshot(self, ts: float):
        """
        Return the nearest option-chain snapshot with chain.timestamp <= ts.
        This is the v4 anti-lookahead alignment mechanism.
        We choose the *nearest expiry* chain for now.
        """
        # Collect all chains that have a timestamp ≤ ts
        candidates = [
            chain
            for chain in self.chains.values()
            if chain.timestamp is not None and float(chain.timestamp) <= ts
        ]

        if not candidates:
            return None

        # Choose the latest available chain (max timestamp ≤ ts)
        chain = max(candidates, key=lambda c: float(c.timestamp or 0.0))

        # Use the v4 bridge on OptionChain to build a timestamp-aligned snapshot
        return chain.to_snapshot(engine_ts=ts)

    def window(self, ts: float, n: int):
        """
        Return the last n chains with timestamp <= ts.
        This mirrors OHLCV/orderbook window() semantics.
        """
        # Select valid chains
        valid = [
            c for c in self.chains.values()
            if c.timestamp is not None and float(c.timestamp) <= ts
        ]

        if not valid:
            return []

        # Sort by timestamp and take the last n
        valid_sorted = sorted(valid, key=lambda c: float(c.timestamp or 0.0))
        tail = valid_sorted[-n:]

        # Return aligned OptionChainSnapshot objects
        return [chain.to_snapshot(engine_ts=ts) for chain in tail]

    def snapshot_by_expiry(self, expiry: str) -> pd.DataFrame:
        """
        Return df snapshot for specific expiry.
        """
        chain = self.chains.get(expiry)
        if chain is None:
            return pd.DataFrame()
        return chain.to_dataframe()

    # ----------------------------------------------------------------------
    # 4d. Snapshot for all expiries
    # ----------------------------------------------------------------------
    def snapshot_all(self) -> Dict[str, pd.DataFrame]:
        """
        Return dict of expiry -> DataFrame.
        """
        return {
            expiry: chain.to_dataframe()
            for expiry, chain in self.chains.items()
        }

    # ----------------------------------------------------------------------
    # 4e. Get a specific contract
    # ----------------------------------------------------------------------
    def get_contract(self, expiry: str, strike: float, option_type: OptionType):
        """
        Unified accessor for single option contract.
        """
        chain = self.chains.get(expiry)
        if chain is None:
            return None
        return chain.get_contract(strike, option_type)

    # ----------------------------------------------------------------------
    # 4f. Nearest non-expired expiry
    # ----------------------------------------------------------------------
    def get_nearest_expiry(self, current_timestamp: str) -> Optional[str]:
        """
        Return nearest expiry >= current_timestamp.
        Assumes ISO-like sortable timestamp strings.
        """
        valid = [e for e in self.chains.keys() if e >= current_timestamp]
        if not valid:
            return None
        return sorted(valid)[0]

    # ----------------------------------------------------------------------
    # 4g. Expiry after X days (simple version)
    # ----------------------------------------------------------------------
    def get_expiry_after(self, days: int) -> Optional[str]:
        """
        Find an expiry approximately days ahead.
        Requires expiry strings to be sortable.
        """
        expiries = sorted(self.chains.keys())
        if not expiries:
            return None
        # naive: choose index = days
        idx = min(days, len(expiries) - 1)
        return expiries[idx]

    # ----------------------------------------------------------------------
    # 4h. Flatten all chains for debugging or saving
    # ----------------------------------------------------------------------
    def dump_to_dataframe(self) -> pd.DataFrame:
        """
        Flatten all chains into a long dataframe.
        """
        frames = []
        for expiry, chain in self.chains.items():
            frames.append(chain.to_dataframe())
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ----------------------------------------------------------------------
    # 4i. Incremental tick update helper
    # ----------------------------------------------------------------------
    def on_tick(self, tick: Dict):
        """
        Update single option contract from tick feed.
        Expected fields: {'expiry','strike','type','bid','ask','iv','delta',...}
        """
        # Extract raw fields
        expiry_raw = tick.get("expiry")
        strike_raw = tick.get("strike")
        type_raw = tick.get("type")
        # ---- Type validation ----
        if not isinstance(expiry_raw, str):
            log_debug(self._logger, "Tick ignored: invalid expiry", expiry=expiry_raw)
            return
        try:
            opt_type = OptionType(type_raw)
        except Exception:
            log_debug(self._logger, "Tick ignored: invalid option type", type=type_raw)
            return
                # Remaining fields passed directly
        fields = {
            k: v for k, v in tick.items()
            if k not in ("expiry", "strike", "type")
        }
        assert isinstance(strike_raw, (int, float)), "Invalid strike in tick"
        # ---- Safe update ----
        self.update_contract(
            expiry=expiry_raw,
            strike=float(strike_raw),
            option_type=opt_type,
            **fields
        )

    # ----------------------------------------------------------------------
    # 5b. Cleanup expired with robust timestamp handling
    # ----------------------------------------------------------------------
    def cleanup_expired(self, current_timestamp: str):
        """
        Remove chains whose expiry < current_timestamp.
        Assumes sortable timestamp format (ISO-like).
        """
        expired = [e for e in self.chains.keys() if e < current_timestamp]
        for e in expired:
            log_info(self._logger, "Removing expired option chain", expiry=e)
            del self.chains[e]

    # ----------------------------------------------------------------------
    # 6. Internal helper
    # ----------------------------------------------------------------------
    def _df_to_chain(self, df: pd.DataFrame) -> OptionChain:
        symbol = df['symbol'].iloc[0]
        expiry = str(df['expiry'].iloc[0])

        contracts = []
        for _, row in df.iterrows():
            contracts.append(
                OptionContract(
                    symbol=symbol,
                    expiry=expiry,
                    strike=row['strike'],
                    option_type=OptionType(row['type']),
                    bid=row.get('bid'),
                    ask=row.get('ask'),
                    last=row.get('last'),
                    volume=row.get('volume'),
                    open_interest=row.get("oi") or row.get("open_interest"),
                    implied_vol=row.get('iv'),
                    delta=row.get('delta'),
                    gamma=row.get('gamma'),
                    vega=row.get('vega'),
                    theta=row.get('theta'),
                )
            )

        chain = OptionChain(symbol=symbol, expiry=expiry, contracts=contracts)
        # NEW — derive timestamp from DataFrame if present
        if "timestamp" in df.columns:
            try:
                ts = float(df["timestamp"].iloc[0])
                chain.set_timestamp(ts)
            except Exception:
                pass
        return chain