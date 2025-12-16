

import pandas as pd
from quant_engine.data.orderbook.cache import OrderbookCache
from quant_engine.data.orderbook.snapshot import OrderbookSnapshot
from quant_engine.utils.logger import get_logger, log_debug, log_info


class HistoricalOrderbookHandler:
    """
    Historical L1/L2 orderbook loader.
    Mirrors the structure of HistoricalDataHandler (OHLCV),
    but operates on orderbook snapshots instead of OHLCV bars.
    """

    def __init__(self, path: str, symbol: str, window: int = 200):
        self.path = path
        self.symbol = symbol
        self.cache = OrderbookCache(window=window)
        self.data = None
        self._logger = get_logger(__name__)

    # ----------------------------------------------------------------------
    # Fetching raw historical orderbook data (to be implemented by user)
    # ----------------------------------------------------------------------
    def fetch_from_api(self, *, exchange: str = "BINANCE", limit: int = 1000):
        """
        Placeholder:
        Fetch historical orderbook snapshots from exchange API.

        Expected return:
            A DataFrame with columns:
            [timestamp, best_bid, best_bid_size, best_ask, best_ask_size]
        """
        raise NotImplementedError("fetch_from_api() not implemented yet.")

    def save_to_csv(self, path: str):
        """
        Save loaded or fetched historical orderbook to CSV.
        """
        if self.data is None:
            raise ValueError("No data loaded/fetched to save.")
        self.data.to_csv(path, index=False)
 
    @classmethod
    def from_api(cls, *, symbol: str, window: int = 200, exchange: str = "BINANCE", limit: int = 1000):
        """
        Alternative constructor:
        Create handler by fetching data directly from exchange API.
        """
        obj = cls(path="", symbol=symbol, window=window)
        df = obj.fetch_from_api(exchange=exchange, limit=limit)
        obj.data = df
        return obj
    @classmethod
    def from_dataframe(cls, df, symbol: str, window: int = 200):
        """
        Construct HistoricalOrderbookHandler directly from DataFrame.
        Expected columns:
            timestamp, best_bid, best_bid_size, best_ask, best_ask_size
        """
        obj = cls(path="", symbol=symbol, window=window)
        obj.data = df.copy()
        obj.cache = OrderbookCache(window=window)
        return obj

    # ----------------------------------------------------------------------
    def load(self):
        """
        Load CSV or Parquet representing historical orderbook snapshots.
        """
        if self.path == "":
            return self.data

        log_debug(self._logger, "HistoricalOrderbookHandler loading file", path=self.path)

        if self.path.endswith(".csv"):
            self.data = pd.read_csv(self.path)
        else:
            raise ValueError("Unsupported file format for orderbook history.")

        # timestamp ordering
        self.data = self.data.sort_values("timestamp")
        log_info(self._logger, "HistoricalOrderbookHandler loaded data", rows=len(self.data))

        return self.data

    # ----------------------------------------------------------------------
    def iter_snapshots(self):
        """
        Yield historical snapshots as OrderbookSnapshot (not DataFrame).
        """
        log_debug(self._logger, "HistoricalOrderbookHandler iterating snapshots")

        if self.data is None:
            self.load()

        assert self.data is not None

        for _, row in self.data.iterrows():
            df_row = row.to_frame().T
            snapshot = OrderbookSnapshot.from_dataframe(df_row, symbol=self.symbol)
            yield snapshot

    # ----------------------------------------------------------------------
    def stream(self):
        """
        Generator: yield snapshot, update rolling cache, return window (list of snapshots).
        """
        log_debug(self._logger, "HistoricalOrderbookHandler streaming snapshots")

        if self.data is None:
            self.load()

        assert self.data is not None

        for _, row in self.data.iterrows():
            df_row = row.to_frame().T
            snapshot = OrderbookSnapshot.from_dataframe(df_row, symbol=self.symbol)

            # update rolling cache
            self.cache.update(snapshot)

            log_debug(
                self._logger,
                "HistoricalOrderbookHandler updated cache",
                latest_timestamp=row["timestamp"],
            )

            yield snapshot, self.cache.get_window()

    # ----------------------------------------------------------------------
    def latest_snapshot(self):
        """Return most recent snapshot object."""
        return self.cache.latest()

    def last_timestamp(self):
        """Return timestamp of most recent cached snapshot."""
        s = self.cache.latest()
        return None if s is None else s.timestamp

    def window(self, n: int | None = None):
        """Return list of snapshots (full window or last n)."""
        window = self.cache.get_window()
        if n is not None:
            return window[-n:]
        return window