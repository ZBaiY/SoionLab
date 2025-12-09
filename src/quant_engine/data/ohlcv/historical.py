import pandas as pd
from quant_engine.utils.logger import get_logger, log_debug, log_info


class HistoricalDataHandler:
    """
    Pure historical data loader — no caching.

    Responsibilities:
        - Load OHLCV history
        - Yield bars one-by-one for BacktestEngine
        - Nothing else
    """

    def __init__(self, path: str):
        self.path = path
        self.data = None
        self._logger = get_logger(__name__)

    @classmethod
    def from_dataframe(cls, df):
        """Useful for tests and synthetic examples."""
        obj = cls(path="")
        obj.data = df.copy()
        return obj

    def load(self):
        """Load CSV or Parquet historical data."""
        if self.data is not None:
            return self.data

        if self.path.endswith(".csv"):
            self.data = pd.read_csv(self.path)
        else:
            raise ValueError("Unsupported file format")

        # ensure timestamps sorted
        self.data = self.data.sort_values("timestamp")
        log_info(self._logger, "Historical loaded", rows=len(self.data))
        return self.data

    def iter_bars(self):
        """Yield bars one-by-one                     as DataFrame(1 row)."""
        if self.data is None:
            self.load()
        assert self.data is not None
        log_debug(self._logger, "HistoricalDataHandler iter_bars() called", rows=len(self.data))
        for _, row in self.data.iterrows():
            yield row.to_frame().T