

from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any, Iterable, AsyncIterable, Iterator, AsyncIterator
from pathlib import Path

from ingestion.contracts.source import Source, AsyncSource, Raw

"""
payload keys expected:
    - timestamp: pd.Timestamp / datetime / str
    - timestamp_ms: epoch milliseconds int (optional alternative)
    - _BINANCE_KLINE_SCHEMA (including closed_time)
"""


class OHLCVFileSource(Source):
    """
    OHLCV source backed by local parquet files.

    Layout:
        root/
          └── <symbol>/
              └── <interval>/
                  ├── 2023.parquet
                  ├── 2024.parquet
                  └── ...
    """

    def __init__(self, *, root: str | Path, **kwargs):
        self._root = Path(root)
        self._symbol = kwargs.get("symbol")
        self._interval = kwargs.get("interval")
        assert self._symbol is not None and isinstance(self._symbol, str), "symbol must be provided as a string"
        assert self._interval is not None and isinstance(self._interval, str), "interval must be provided as a string"
        self._interval_ms = self._interval_milliseconds(self._interval)
        self._path = self._root / self._symbol / self._interval
        if not self._path.exists():
            raise FileNotFoundError(f"OHLCV path does not exist: {self._path}")

    @staticmethod
    def _interval_milliseconds(interval: str) -> int:
        # supports: 1m/15m/1h/4h/1d/1w
        n = int(interval[:-1])
        u = interval[-1].lower()
        if u == "m":
            return n * 60_000
        if u == "h":
            return n * 3_600_000
        if u == "d":
            return n * 86_400_000
        if u == "w":
            return n * 7 * 86_400_000
        raise ValueError(f"Unsupported interval: {interval!r}")

    @staticmethod
    def _coerce_ts(x: Any) -> int:
        """Return unix epoch milliseconds as int. Accepts seconds/ms, datetime, pandas Timestamp, str."""
        if x is None:
            raise ValueError("timestamp is None")

        if isinstance(x, bool):
            raise ValueError("invalid timestamp type: bool")

        if isinstance(x, (int, float)):
            v = float(x)
            # heuristic: seconds ~1e9, ms ~1e12
            if v < 10_000_000_000:
                return int(round(v * 1000.0))
            return int(round(v))

        if isinstance(x, datetime):
            dt = x if x.tzinfo else x.replace(tzinfo=timezone.utc)
            return int(round(dt.timestamp() * 1000.0))

        # pandas.Timestamp / numpy datetime64 / ISO str / etc.
        import pandas as pd  # local import to keep module optional

        ts = pd.to_datetime(x, utc=True, errors="raise")
        if hasattr(ts, "to_pydatetime"):
            return int(round(ts.to_pydatetime().timestamp() * 1000.0))
        return int(round(pd.Timestamp(ts).to_pydatetime().timestamp() * 1000.0))

    def __iter__(self) -> Iterator[Raw]:
        try:
            import pandas as pd
        except ImportError as e:
            raise RuntimeError("pandas is required for OHLCVFileSource parquet loading") from e

        files = sorted(self._path.glob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"No parquet files found under {self._path}")

        dt = int(self._interval_ms)

        for fp in files:
            df = pd.read_parquet(fp)

            # ---- canonical timestamp: close-time to avoid lookahead ----
            if "timestamp" not in df.columns:
                if "close_time" in df.columns:
                    df["timestamp"] = df["close_time"].map(self._coerce_ts)
                elif "closed_time" in df.columns:
                    df["timestamp"] = df["closed_time"].map(self._coerce_ts)
                elif "open_time" in df.columns:
                    # data available at bar close => open_time + interval
                    df["timestamp"] = df["open_time"].map(self._coerce_ts) + int(dt)
                else:
                    raise ValueError(
                        f"Parquet file {fp} missing 'timestamp' and no fallback "
                        f"('close_time'/'closed_time'/'open_time') columns found"
                    )
            else:
                df["timestamp"] = df["timestamp"].map(self._coerce_ts)

            df = df.sort_values("timestamp", kind="mergesort")
            df["timestamp"] = df["timestamp"].astype("int64", copy=False)

            # pandas types: dict[Hashable, Any] -> dict[str, Any]
            for rec in df.to_dict(orient="records"):
                yield {str(k): v for k, v in rec.items()}

class OHLCVRESTSource(Source):
    """
    OHLCV source using REST-style polling.
    """

    def __init__(
        self,
        *,
        fetch_fn,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
    ):
        """
        Parameters
        ----------
        fetch_fn:
            Callable returning an iterable of raw OHLCV payloads.
            The function itself handles authentication / pagination.
        poll_interval:
            Backward-compatible polling cadence in seconds.
        poll_interval_ms:
            Polling cadence in epoch milliseconds.
        interval:
            Polling cadence as an interval string (e.g. "250ms", "1s", "1m").
            If provided, it takes precedence over poll_interval.
        """
        self._fetch_fn = fetch_fn

        if poll_interval_ms is not None:
            self._interval_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            self._interval_ms = int(round(float(poll_interval) * 1000.0))
            # self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            raise ValueError("One of poll_interval, poll_interval_ms, or interval must be provided")

        if self._interval_ms <= 0:
            raise ValueError(f"poll interval must be > 0ms, got {self._interval_ms}")

    def __iter__(self) -> Iterator[Raw]:
        while True:
            rows = self._fetch_fn()
            for row in rows:
                yield row
            time.sleep(self._interval_ms / 1000.0)


class OHLCVWebSocketSource(AsyncSource):
    """
    OHLCV source backed by a WebSocket async iterator.

    Intended for:
        - realtime streaming klines
    """

    def __init__(self, stream: AsyncIterable[Raw] | None = None):
        self._stream = stream

    def __aiter__(self) -> AsyncIterator[Raw]:
        async def _gen():
            assert self._stream is not None, "stream must be provided for OHLCVWebSocketSource"
            async for msg in self._stream:
                yield msg

        return _gen()