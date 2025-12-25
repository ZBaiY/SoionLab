from __future__ import annotations
from typing import Any, Optional
import pandas as pd
from quant_engine.data.sentiment.snapshot import SentimentSnapshot
from quant_engine.data.sentiment.cache import SentimentCache
from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler, to_interval_ms
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
    interval_ms: int

    bootstrap_cfg: dict[str, Any]
    cache_cfg: dict[str, Any]
    cache: SentimentCache

    _anchor_ts: int | None
    _logger: Any

    def __init__(self, symbol: str, **kwargs: Any):
        self.symbol = symbol
        self._logger = get_logger(__name__)

        interval = kwargs.get("interval")
        if not isinstance(interval, str) or not interval:
            raise ValueError("Sentiment handler requires non-empty 'interval' (e.g. '15m')")
        self.interval = interval
        interval_ms = to_interval_ms(self.interval) if self.interval is not None else None
        if self.interval is not None and interval_ms is None:
            raise ValueError(f"Invalid interval format: {self.interval}")
        self.interval_ms = int(interval_ms) if interval_ms is not None else 0

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

    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
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
    def align_to(self, ts: int) -> None:
        self._anchor_ts = int(ts)
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

    def last_timestamp(self) -> int | None:
        s = self.cache.latest()
        if s is None:
            return None
        last_ts = int(s.timestamp)
        if self._anchor_ts is not None:
            return min(last_ts, int(self._anchor_ts))
        return last_ts

    def get_snapshot(self, ts: int | None = None) -> Optional[SentimentSnapshot]:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        return self.cache.latest_before_ts(int(t))

    def window(self, ts: int | None = None, n: int = 1) -> list[SentimentSnapshot]:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return []
        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        return self.cache.window_before_ts(int(t), int(n))

    def reset(self) -> None:
        self.cache.clear()
    

    def _coerce_snapshot(self, item: Any) -> SentimentSnapshot | None:
        if isinstance(item, SentimentSnapshot):
            return item

        if not isinstance(item, dict):
            return None

        obs_ts = _coerce_ts(item.get("obs_ts", item.get("timestamp", item.get("ts"))))
        if obs_ts is None:
            return None
        obs_ts_i = int(obs_ts)

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
            engine_ts=obs_ts_i,   # engine_ts will be clamped by align_to
            obs_ts=obs_ts_i,
            symbol=self.symbol,
            model=self.model,
            score=score_f,
            embedding=embedding,
            meta=meta_d,
        )
        

def _coerce_ts(x: Any) -> int | None:
    if x is None:
        return None
    if isinstance(x, bool):
        return None
    if isinstance(x, int):
        return int(x)
    if isinstance(x, float):
        return int(x)
    if isinstance(x, pd.Timestamp):
        # pandas Timestamp is ns since epoch
        try:
            return int(x.value // 1_000_000)
        except Exception:
            return int(x.timestamp() * 1000)
    if isinstance(x, dict):
        v = x.get("timestamp", x.get("ts"))
        return _coerce_ts(v)
    try:
        return int(x)  # strings, numpy scalars
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None
