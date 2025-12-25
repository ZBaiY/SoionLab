from __future__ import annotations

from collections import deque
from typing import Any, Deque, Optional, Iterable

from quant_engine.utils.logger import get_logger, log_debug

from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler, to_interval_ms
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.derivatives.option_chain.snapshot import OptionChainSnapshot
from quant_engine.data.derivatives.iv.snapshot import IVSurfaceSnapshot


class IVSurfaceDataHandler(RealTimeDataHandler):
    """Runtime IV surface handler (derived layer, mode-agnostic).

    Contract / protocol shadow:
      - kwargs-driven __init__ (loader/builder passes handler config via **cfg)
      - bootstrap(end_ts, lookback) present (no-op by default; IO-free)
      - warmup_to(ts) establishes anti-lookahead anchor
      - get_snapshot(ts=None) / window(ts=None,n) are timestamp-aligned
      - last_timestamp() supported

    Semantics:
      - This handler is a *derived* layer from an underlying OptionChainDataHandler.
      - It does not touch exchange APIs; it only converts OptionChainSnapshot -> IVSurfaceSnapshot.
      - A real SABR/SSVI calibrator can be plugged later without changing this API.

    Config (Strategy.DATA.*.iv_surface):
      - source: routing/metadata (default: "deribit")
      - interval: required cadence (e.g. "1m", "5m")
      - bootstrap.lookback: convenience horizon for Engine.bootstrap()
      - cache.max_bars: in-memory cache depth (IVSurfaceSnapshot)
      - expiry: optional expiry selector (future)
      - model_name: optional label (e.g. "SSVI", "SABR", "CHAIN_DERIVED")

    NOTE: `chain_handler` must be provided via kwargs at construction time by the builder.
    """

    # --- declared attributes (protocol/typing shadow) ---
    symbol: str
    chain_handler: OptionChainDataHandler
    interval: str
    bootstrap_cfg: dict[str, Any]
    cache_cfg: dict[str, Any]
    expiry: str | None
    model_name: str
    interval_ms: int

    _snapshots: Deque[IVSurfaceSnapshot]
    _anchor_ts: int | None
    _logger: Any

    def __init__(self, symbol: str, **kwargs: Any):
        self.symbol = symbol
        self._logger = get_logger(__name__)

        # required: chain_handler
        ch = kwargs.get("chain_handler") or kwargs.get("option_chain_handler")
        if not isinstance(ch, OptionChainDataHandler):
            raise ValueError("IVSurfaceDataHandler requires 'chain_handler' (OptionChainDataHandler) in kwargs")
        self.chain_handler = ch

        # required: interval
        ri = kwargs.get("interval")
        if not isinstance(ri, str) or not ri:
            raise ValueError("IV surface handler requires non-empty 'interval' (e.g. '5m')")
        self.interval = ri
        ri_ms = to_interval_ms(self.interval)
        if ri_ms is None:
            raise ValueError(f"Invalid interval format: {self.interval}")
        self.interval_ms = int(ri_ms)

        # optional model/expiry
        expiry = kwargs.get("expiry")
        if expiry is not None and (not isinstance(expiry, str) or not expiry):
            raise ValueError("IV surface 'expiry' must be a non-empty string if provided")
        self.expiry = expiry

        model_name = kwargs.get("model_name", kwargs.get("model", "CHAIN_DERIVED"))
        if not isinstance(model_name, str) or not model_name:
            raise ValueError("IV surface 'model_name' must be a non-empty string")
        self.model_name = model_name

        # optional nested configs
        bootstrap = kwargs.get("bootstrap") or {}
        if not isinstance(bootstrap, dict):
            raise TypeError("IV surface 'bootstrap' must be a dict")
        self.bootstrap_cfg = dict(bootstrap)

        cache = kwargs.get("cache") or {}
        if not isinstance(cache, dict):
            raise TypeError("IV surface 'cache' must be a dict")
        self.cache_cfg = dict(cache)

        max_bars = self.cache_cfg.get("max_bars")
        if max_bars is None:
            max_bars = kwargs.get("window", 1000)
        max_bars_i = int(max_bars)
        if max_bars_i <= 0:
            raise ValueError("IV surface cache.max_bars must be > 0")

        self._snapshots = deque(maxlen=max_bars_i)
        self._anchor_ts = None

        log_debug(
            self._logger,
            "IVSurfaceDataHandler initialized",
            symbol=self.symbol,
            interval=self.interval,
            max_bars=max_bars_i,
            bootstrap=self.bootstrap_cfg,
            model_name=self.model_name,
            expiry=self.expiry,
        )


    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        if lookback is None:
            lookback = self.bootstrap_cfg.get("lookback")
        log_debug(
            self._logger,
            "IVSurfaceDataHandler.bootstrap (no-op)",
            symbol=self.symbol,
            anchor_ts=anchor_ts,
            lookback=lookback,
        )

    # align_to(ts) defines the maximum visible engine-time for all read APIs.
    def align_to(self, ts: int) -> None:
        self._anchor_ts = int(ts)
        log_debug(self._logger, "IVSurfaceDataHandler.align_to", symbol=self.symbol, anchor_ts=self._anchor_ts)

    # ------------------------------------------------------------------
    # Derived update (called by engine/driver when appropriate)
    # ------------------------------------------------------------------

    def on_new_tick(self, bar: Any) -> None:
        """
        Ingest a derived-tick trigger for IV surface generation.

        Semantics:
          - This handler is *derived* from OptionChainDataHandler.
          - `bar` is treated as a trigger carrying an engine timestamp only.
          - No raw market data is ingested here.
          - Snapshot derivation pulls from chain_handler.get_snapshot(ts).
          - Visibility is enforced exclusively via align_to(ts).
        """
        ts = _coerce_ts(bar)
        if ts is None:
            return
        snap = self._derive_from_chain(ts)
        if snap is not None:
            self._snapshots.append(snap)

    # Derive IV surface strictly from visible option-chain state at ts.
    def _derive_from_chain(self, ts: int) -> IVSurfaceSnapshot | None:
        chain_snap: OptionChainSnapshot | None = self.chain_handler.get_snapshot(ts)
        if chain_snap is None:
            return None

        surface_ts = int(getattr(chain_snap, "timestamp", ts))
        atm_iv = float(getattr(chain_snap, "atm_iv", 0.0))
        skew = float(getattr(chain_snap, "skew", 0.0))
        curve = dict(getattr(chain_snap, "smile", {}))

        # Enforce timestamp = engine ts, data_ts = surface_ts
        return IVSurfaceSnapshot.from_surface_aligned(
            timestamp=ts,
            data_ts=surface_ts,
            atm_iv=atm_iv,
            skew=skew,
            curve=curve,
            surface={},
            symbol=self.symbol,
            expiry=self.expiry,
            model=self.model_name,
        )

    # ------------------------------------------------------------------
    # v4 timestamp-aligned API
    # ------------------------------------------------------------------

    def last_timestamp(self) -> int | None:
        if not self._snapshots:
            return None
        last_ts = int(self._snapshots[-1].timestamp)
        if self._anchor_ts is not None:
            return min(last_ts, int(self._anchor_ts))
        return last_ts

    def get_snapshot(self, ts: int | None = None) -> Optional[IVSurfaceSnapshot]:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None

        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        for s in reversed(self._snapshots):
            if int(s.timestamp) <= t:
                return s
        return None

    def window(self, ts: int | None = None, n: int = 1) -> list[IVSurfaceSnapshot]:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return []

        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        out: list[IVSurfaceSnapshot] = []
        for s in reversed(self._snapshots):
            if int(s.timestamp) <= t:
                out.append(s)
                if len(out) >= int(n):
                    break
        out.reverse()
        return out


def _coerce_ts(x: Any) -> int | None:
    if x is None:
        return None
    if isinstance(x, bool):  # bool is a subclass of int; exclude
        return None
    if isinstance(x, int):
        return int(x)
    if isinstance(x, float):
        # Allow float inputs but treat as ms epoch if already ms-like
        return int(x)
    if isinstance(x, dict):
        v = x.get("timestamp", x.get("ts"))
        if v is None:
            return None
        try:
            return int(v)
        except Exception:
            try:
                return int(float(v))
            except Exception:
                return None
    return None
