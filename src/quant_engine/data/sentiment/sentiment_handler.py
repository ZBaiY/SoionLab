from __future__ import annotations
from typing import Any, Optional
import pandas as pd
from quant_engine.data.sentiment.snapshot import SentimentSnapshot
from quant_engine.data.sentiment.cache import SentimentCache
from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler, TimestampLike
from quant_engine.data.sentiment.historical import HistoricalSentimentHandler
from quant_engine.utils.logger import get_logger, log_debug


# TODO : sentiment calculation, pipeline integration, more tests

class SentimentHandler(RealTimeDataHandler):
    """Runtime sentiment handler (mode-agnostic).

    Protocol shadow: subclasses `RealTimeDataHandler`.

    Config (Strategy.DATA.*.sentiment):
      - source: required (e.g. "news", "twitter", "onchain")
      - interval: required cadence (e.g. "15m")
      - model: required (e.g. "embedding", "lexicon", "llm")
      - bootstrap.lookback: convenience horizon for Engine.bootstrap()
      - cache.max_bars: in-memory cache depth (SentimentSnapshot)

    IO boundary:
      - IO-free by default; upstream fetcher lives elsewhere.

    Anti-lookahead:
      - warmup_to(ts) sets anchor; get_snapshot/window clamp to it by default.
    """

    symbol: str
    interval: str
    model: str

    bootstrap_cfg: dict[str, Any]
    cache_cfg: dict[str, Any]
    cache: SentimentCache

    _anchor_ts: float | None
    _logger: Any

    def __init__(self, symbol: str, **kwargs: Any):
        self.symbol = symbol
        self._logger = get_logger(__name__)

        interval = kwargs.get("interval")
        if not isinstance(interval, str) or not interval:
            raise ValueError("Sentiment handler requires non-empty 'interval' (e.g. '15m')")
        self.interval = interval

        model = kwargs.get("model")
        if not isinstance(model, str) or not model:
            raise ValueError("Sentiment handler requires non-empty 'model' (e.g. 'embedding')")
        self.model = model

        bootstrap = kwargs.get("bootstrap") or {}
        if not isinstance(bootstrap, dict):
            raise TypeError("Sentiment 'bootstrap' must be a dict")
        self.bootstrap_cfg = dict(bootstrap)

        cache = kwargs.get("cache") or {}
        if not isinstance(cache, dict):
            raise TypeError("Sentiment 'cache' must be a dict")
        self.cache_cfg = dict(cache)

        max_bars = self.cache_cfg.get("max_bars")
        if max_bars is None:
            max_bars = kwargs.get("window", 1000)
        max_bars_i = int(max_bars)
        if max_bars_i <= 0:
            raise ValueError("Sentiment cache.max_bars must be > 0")

        self.cache = SentimentCache(max_bars=max_bars_i)
        self._anchor_ts = None

        log_debug(
            self._logger,
            "SentimentHandler initialized",
            symbol=self.symbol,
            interval=self.interval,
            model=self.model,
            max_bars=max_bars_i,
            bootstrap=self.bootstrap_cfg,
        )

    # ------------------------------------------------------------------
    # Lifecycle (realtime/mock)
    # ------------------------------------------------------------------

    def bootstrap(self, *, anchor_ts: float | None = None, lookback: Any | None = None) -> None:
        if lookback is None:
            lookback = self.bootstrap_cfg.get("lookback")
        log_debug(
            self._logger,
            "SentimentHandler.bootstrap (no-op)",
            symbol=self.symbol,
            anchor_ts=anchor_ts,
            lookback=lookback,
        )

    # align_to(ts) defines the maximum visible engine-time for all read APIs.
    def align_to(self, ts: float) -> None:
        self._anchor_ts = float(ts)
        log_debug(self._logger, "SentimentHandler align_to", symbol=self.symbol, anchor_ts=self._anchor_ts)

    # ------------------------------------------------------------------
    # Streaming ingestion
    # ------------------------------------------------------------------

    def on_new_tick(self, payload: Any) -> None:
        """
        Ingest a sentiment payload (event-time fact).

        Payload contract:
          - Represents an already-occurred observation (event-time).
          - May be:
              * SentimentSnapshot
              * dict with resolvable observation timestamp
          - Ingest is append-only and unconditional.
          - No visibility or engine-time decisions are made here.
        """
        snap = self._coerce_snapshot(payload)
        if snap is None:
            return
        self.cache.update(snap)

    # ------------------------------------------------------------------
    # Timestamp-aligned access
    # ------------------------------------------------------------------

    def last_timestamp(self) -> float | None:
        s = self.cache.latest()
        if s is None:
            return None
        last_ts = float(s.timestamp)
        if self._anchor_ts is not None:
            return min(last_ts, float(self._anchor_ts))
        return last_ts

    def get_snapshot(self, ts: float | None = None) -> Optional[SentimentSnapshot]:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None
        if self._anchor_ts is not None:
            ts = min(float(ts), float(self._anchor_ts))
        return self.cache.latest_before_ts(float(ts))

    def window(self, ts: float | None = None, n: int = 1) -> list[SentimentSnapshot]:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return []
        if self._anchor_ts is not None:
            ts = min(float(ts), float(self._anchor_ts))
        return self.cache.window_before_ts(float(ts), int(n))

    def reset(self) -> None:
        self.cache.clear()
    

    def _coerce_snapshot(self, item: Any) -> SentimentSnapshot | None:
        if isinstance(item, SentimentSnapshot):
            return item

        if not isinstance(item, dict):
            return None

        obs_ts = item.get("obs_ts", item.get("timestamp", item.get("ts")))
        if obs_ts is None:
            return None
        try:
            obs_ts_f = float(obs_ts)
        except Exception:
            return None

        score = item.get("score", 0.0)
        try:
            score_f = float(score)
        except Exception:
            score_f = 0.0

        emb = item.get("embedding")
        if emb is not None and not isinstance(emb, list):
            emb = None
        embedding = [float(x) for x in emb] if isinstance(emb, list) else None

        meta = item.get("meta")
        meta_d = meta if isinstance(meta, dict) else {}

        return SentimentSnapshot.from_payload(
            engine_ts=obs_ts_f,   # engine_ts will be clamped by align_to
            obs_ts=obs_ts_f,
            symbol=self.symbol,
            model=self.model,
            score=score_f,
            embedding=embedding,
            meta=meta_d,
        )
        

def _to_float_ts(ts: "TimestampLike | None") -> float | None:
    if ts is None:
        return None
    if isinstance(ts, pd.Timestamp):
        return float(ts.timestamp())
    return float(ts)

def _coerce_ts(x: Any) -> float | None:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, dict):
        v = x.get("timestamp", x.get("ts"))
        if v is None:
            return None
        try:
            return float(v)
        except Exception:
            return None
    return None
