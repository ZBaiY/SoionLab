from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterable, AsyncIterator, Iterator
import pandas as pd

import requests
from quant_engine.data.contracts.protocol_realtime import to_interval_ms
from ingestion.contracts.source import Source, AsyncSource, Raw

def _now_ms() -> int:
    return int(time.time() * 1000.0)


def _date_path(root: Path, *, interval: str, asset: str, data_ts: int) -> Path:
    dt = datetime.fromtimestamp(int(data_ts) / 1000.0, tz=timezone.utc)
    year = dt.strftime("%Y")
    ymd = dt.strftime("%Y_%m_%d")
    return root / asset / interval / year / f"{ymd}.parquet"

class OptionChainFileSource(Source):
    """
    Option chain source backed by local parquet snapshots.

    Layout:
        data/raw/option_chain/<ASSET>/<INTERVAL>/<YYYY>/<YYYY>_<MM>_<DD>.parquet.
    """

    def __init__(self, *, root: str | Path, asset: str, interval: str | None = None):
        self._root = Path(root)
        self._asset = str(asset)
        self._path = self._root / self._asset
        if interval is not None:
            self._path = self._path / str(interval)
        if not self._path.exists():
            raise FileNotFoundError(f"Option chain path does not exist: {self._path}")

    def __iter__(self) -> Iterator[Raw]:
        try:
            import pandas as pd
        except ImportError as e:
            raise RuntimeError("pandas is required for OptionChainFileSource parquet loading") from e

        files = sorted(self._path.rglob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"No option chain parquet files found under {self._path}")

        for fp in files:
            df = pd.read_parquet(fp)
            if df is None or df.empty:
                continue
            if "data_ts" not in df.columns:
                raise ValueError(f"Parquet file {fp} missing data_ts column")

            df = df.sort_values(["data_ts"], kind="stable")
            for ts, sub in df.groupby("data_ts", sort=True):
                snap = sub.reset_index(drop=True)
                # Source contract: yield a Mapping[str, Any]
                assert isinstance(ts, (int, float)), f"data_ts must be int/float, got {type(ts)!r}"
                yield {"data_ts": int(ts), "frame": snap}


class DeribitOptionChainRESTSource(Source):
    """Deribit option-chain source using REST polling.

    Fetches instrument metadata (kind=option, expired=false) and writes raw
    snapshots under data/raw/option_chain/<ASSET>/<INTERVAL>/<YYYY>/<YYYY>_<MM>_<DD>.parquet.
    """

    def __init__(
        self,
        *,
        currency: str,
        interval: str | None = None,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        base_url: str = "https://www.deribit.com",
        timeout: float = 10.0,
        root: str | Path = "data/raw/option_chain",
        kind: str = "option",
        expired: bool = False,
        max_retries: int = 5,
        backoff_s: float = 1.0,
        backoff_max_s: float = 30.0,
    ):
        
        self._currency = str(currency)
        self._base_url = base_url.rstrip("/")
        self._timeout = float(timeout)
        self.interval = interval if interval is not None else "1m"
        self._kind = str(kind)
        self._expired = bool(expired)
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._max_retries = int(max_retries)
        self._backoff_s = float(backoff_s)
        self._backoff_max_s = float(backoff_max_s)

        if poll_interval_ms is not None:
            self._poll_interval_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        elif interval is not None:
                pms = to_interval_ms(interval)
                assert pms is not None, f"cannot parse interval: {interval!r}"
                self._poll_interval_ms = int(float(pms) / 60_000)
        else:
            self._poll_interval_ms = 60_000

        if self._poll_interval_ms <= 0:
            raise ValueError(f"poll interval must be > 0ms, got {self._poll_interval_ms}")

    def __iter__(self) -> Iterator[Raw]:
        try:
            import pandas as pd
        except ImportError as e:
            raise RuntimeError("pandas is required for DeribitOptionChainRESTSource parquet writing") from e

        while True:
            data_ts = _now_ms()
            backoff = self._backoff_s
            for _ in range(self._max_retries):
                try:
                    rows = self._fetch()
                    df = pd.DataFrame(rows or [])
                    if not df.empty:
                        df["data_ts"] = int(data_ts)
                        self._write_raw_snapshot(df=df, data_ts=int(data_ts))
                    # Source contract: yield a Mapping[str, Any]
                    yield {"data_ts": int(data_ts), "frame": df}
                    break
                except Exception:
                    time.sleep(min(backoff, self._backoff_max_s))
                    backoff = min(backoff * 2.0, self._backoff_max_s)
            time.sleep(self._poll_interval_ms / 1000.0)

    def _fetch(self) -> list[dict]:
        url = f"{self._base_url}/api/v2/public/get_instruments"
        params = {
            "currency": self._currency,
            "kind": self._kind,
            "expired": str(self._expired).lower(),
        }
        r = requests.get(url, params=params, timeout=self._timeout)
        r.raise_for_status()
        payload = r.json()
        result = payload.get("result")
        if not isinstance(result, list):
            raise RuntimeError(f"Unexpected Deribit response: {type(result)!r}")
        return result

    def _write_raw_snapshot(self, *, df, data_ts: int) -> None:
        path = _date_path(self._root, interval = self.interval,asset=self._currency, data_ts=data_ts)
        path.parent.mkdir(parents=True, exist_ok=True)

        if path.exists():
            old = pd.read_parquet(path)
            merged = pd.concat([old, df], ignore_index=True)
        else:
            merged = df

        sort_cols = [c for c in ("data_ts", "expiration_timestamp", "strike", "option_type", "instrument_name") if c in merged.columns]
        if sort_cols:
            merged = merged.sort_values(sort_cols, kind="stable")

        merged.to_parquet(path, index=False)


class OptionChainStreamSource(AsyncSource):
    """
    Option chain source backed by an async stream.
    """

    def __init__(self, stream: AsyncIterable[Raw] | None = None):
        self._stream = stream

    def __aiter__(self) -> AsyncIterator[Raw]:
        async def _gen():
            assert self._stream is not None, "stream must be provided for OptionChainStreamSource"
            async for msg in self._stream:
                yield msg

        return _gen()
