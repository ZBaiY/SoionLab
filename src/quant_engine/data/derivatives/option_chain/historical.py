# quant_engine/data/derivatives/historical.py

import pandas as pd
from typing import List, Dict, Optional
from quant_engine.data.derivatives.option_chain.loader import OptionChainLoader
from quant_engine.data.derivatives.option_chain.option_chain import OptionChain
from quant_engine.utils.logger import get_logger, log_debug, log_info


class HistoricalOptionChainHandler:
    """
    Backtest loader for option-chain data.

    Responsibilities:
        • Load historical option chain snapshots
        • Iterate snapshots one-by-one
        • Provide window snapshots for IV/feature use
    """

    def __init__(self, path: str = "", window: int = 20):
        self.path = path
        self.window = window
        self._loader = OptionChainLoader()
        self.snapshots: List[Dict] = []   # each {"chains":[...], "timestamp": ts}
        self.ptr = 0                      # iterator pointer
        self._logger = get_logger(self.__class__.__name__)

    # ---------------------------------------------------------
    # Constructors
    # ---------------------------------------------------------
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, window: int = 20):
        """
        Build directly from DataFrame.
        """
        obj = cls(path="", window=window)
        loader = OptionChainLoader()

        # group by timestamp to get full chain snapshots
        snapshots = []
        for ts, sub in df.groupby("timestamp"):
            snapshot = loader.load_from_dataframe(sub)
            snapshots.append(snapshot)

        obj.snapshots = snapshots
        return obj

    @classmethod
    def from_api(cls, *, symbol: str, window: int = 20, limit: int = 100):
        """
        Future-proof API constructor.
        """
        obj = cls(path="", window=window)
        obj.fetch_from_api(symbol=symbol, limit=limit)
        return obj

    # ---------------------------------------------------------
    # Load from disk
    # ---------------------------------------------------------
    def load(self):
        if self.path == "":
            return self.snapshots

        log_info(self._logger, "Loading historical option chain CSV", path=self.path)
        df = pd.read_csv(self.path)

        self.snapshots.clear()
        loader = OptionChainLoader()

        for ts, sub in df.groupby("timestamp"):
            snapshot = loader.load_from_dataframe(sub)
            self.snapshots.append(snapshot)

        log_info(self._logger, "Loaded option chain snapshots", count=len(self.snapshots))
        return self.snapshots

    # ---------------------------------------------------------
    # Streaming / Iteration
    # ---------------------------------------------------------
    def iter_snapshots(self):
        """
        Yield snapshots one-at-a-time in chronological order.
        """
        if not self.snapshots:
            self.load()

        for snap in self.snapshots:
            yield snap

    def stream(self):
        """
        Backtest-compatible streaming generator.
        """
        for snap in self.iter_snapshots():
            yield snap, snap["chains"]

    # ---------------------------------------------------------
    # Snapshot Access API
    # ---------------------------------------------------------
    def latest_snapshot(self) -> Optional[Dict]:
        """
        Return latest snapshot in window.
        """
        if not self.snapshots:
            return None
        return self.snapshots[self.ptr - 1] if self.ptr > 0 else None

    def window_snapshot(self, n=None) -> List[Dict]:
        """
        Rolling window of snapshots.
        """
        if n is None:
            n = self.window
        return self.snapshots[max(0, self.ptr - n): self.ptr]

    # ---------------------------------------------------------
    # Fetch from API (placeholder)
    # ---------------------------------------------------------
    def fetch_from_api(self, *, symbol: str, limit: int = 100):
        """
        Placeholder for real exchange API.
        Expected to populate self.snapshots with:
            [{"chains":[...], "timestamp": ts}, ...]
        """
        raise NotImplementedError("fetch_from_api() not implemented yet.")