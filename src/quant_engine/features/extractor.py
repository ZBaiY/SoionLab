from __future__ import annotations
from typing import Optional, Dict, Any, List

from quant_engine.contracts.feature import FeatureChannel
from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.derivatives.iv.iv_handler import IVSurfaceDataHandler
from quant_engine.data.sentiment.sentiment_handler import SentimentDataHandler
from quant_engine.data.orderbook.realtime import RealTimeOrderbookHandler
from .registry import build_feature
from quant_engine.utils.logger import get_logger, log_debug
from quant_engine.data.contracts.protocol_realtime import to_interval_ms

min_warmup = 300

def _parse_feature_name(name: str) -> tuple[str, str, str, str | None]:
    """Parse v4 feature naming convention.

    Expected:
        <TYPE>_<PURPOSE>_<SYMBOL>
        <TYPE>_<PURPOSE>_<SYMBOL>^<REF>

    Example:
        RSI_MODEL_BTCUSDT
        SPREAD_MODEL_BTCUSDT^ETHUSDT
    """
    parts = name.split("_", 2)
    if len(parts) != 3:
        raise ValueError(
            f"Invalid feature name '{name}': expected '<TYPE>_<PURPOSE>_<SYMBOL>' or '<TYPE>_<PURPOSE>_<SYMBOL>^<REF>'"
        )
    ftype, purpose, sym_part = parts
    ref: str | None = None
    symbol = sym_part
    if "^" in sym_part:
        symbol, ref = sym_part.split("^", 1)
        if not symbol or not ref:
            raise ValueError(f"Invalid feature name '{name}': malformed '^' section")
    if not ftype or not purpose or not symbol:
        raise ValueError(f"Invalid feature name '{name}': empty TYPE/PURPOSE/SYMBOL")
    return ftype, purpose, symbol, ref


def _normalize_feature_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and (when safe) infer missing fields from the feature name.

    Rules:
    - item['name'] must exist and follow the convention.
    - item['type'] must exist and match the TYPE in the name.
    - item['symbol'] may be omitted; if so, infer from the name.
    - if name encodes '^REF', ensure params['ref'] is present (infer if missing).
    """
    if not isinstance(item, dict):
        raise TypeError(f"Feature config item must be a dict, got {type(item)!r}")

    name = item.get("name")
    if not name or not isinstance(name, str):
        raise ValueError(f"Feature config missing valid 'name': {item}")

    ftype_in_name, _purpose, symbol_in_name, ref_in_name = _parse_feature_name(name)

    ftype = item.get("type")
    if not ftype or not isinstance(ftype, str):
        raise ValueError(f"Feature '{name}' missing valid 'type'")
    if ftype != ftype_in_name:
        raise ValueError(
            f"Feature '{name}' type mismatch: item.type='{ftype}' but name.TYPE='{ftype_in_name}'"
        )

    symbol = item.get("symbol")
    if symbol is None:
        item["symbol"] = symbol_in_name
    elif symbol != symbol_in_name:
        raise ValueError(
            f"Feature '{name}' symbol mismatch: item.symbol='{symbol}' but name.SYMBOL='{symbol_in_name}'"
        )

    params = item.get("params") or {}
    if not isinstance(params, dict):
        raise TypeError(f"Feature '{name}' params must be a dict, got {type(params)!r}")

    if ref_in_name is not None:
        if "ref" not in params:
            params["ref"] = ref_in_name
        elif params["ref"] != ref_in_name:
            raise ValueError(
                f"Feature '{name}' ref mismatch: params.ref='{params['ref']}' but name.REF='{ref_in_name}'"
            )
    else:
        if "ref" in params:
            raise ValueError(f"Feature '{name}' has params.ref but name has no '^REF' section")

    item["params"] = params
    return item

class FeatureExtractor:
    """
    TradeBot v4 Unified Feature Extractor

    Naming convention (enforced):
        <TYPE>_<PURPOSE>_<SYMBOL>
        <TYPE>_<PURPOSE>_<SYMBOL>^<REF>

    FeatureChannels in v4 operate **only on timestamp-aligned snapshots**.

    Context passed to each FeatureChannel:
        {
            "timestamp": int,     # current timestamp (epoch ms)
            "data": {
                "ohlcv": {symbol → OHLCVHandler},
                "orderbook": {symbol → OrderbookHandler},
                "option_chain": {symbol → OptionChainHandler},
                "iv_surface": {symbol → IVSurfaceDataHandler},
                "sentiment": {symbol → SentimentHandler},
            },
            "warmup_window": Optional[int],
            "ohlcv_window": Optional[pd.DataFrame],
        }

    FeatureChannels then call:
        snapshot(context, data_type)
        window_any(context, data_type, n)

    No feature should ever access handler internals directly.
    """

    _logger = get_logger(__name__)

    def __init__(
        self,
        ohlcv_handlers: Dict[str, OHLCVDataHandler],
        orderbook_handlers: Dict[str, RealTimeOrderbookHandler],
        option_chain_handlers: Dict[str, OptionChainDataHandler],
        iv_surface_handlers: Dict[str, IVSurfaceDataHandler],
        sentiment_handlers: Dict[str, SentimentDataHandler],
        feature_config: List[Dict[str, Any]] | None = None,
    ):
        log_debug(self._logger, "Initializing FeatureExtractor")

        self.ohlcv_handlers = ohlcv_handlers
        self.orderbook_handlers = orderbook_handlers
        self.option_chain_handlers = option_chain_handlers
        self.iv_surface_handlers = iv_surface_handlers
        self.sentiment_handlers = sentiment_handlers

        # Optional: helps choose a stable clock source when multiple symbols exist.
        self._primary_symbol = next(iter(ohlcv_handlers.keys()), "") if ohlcv_handlers else ""

        # Strategy observation interval (engine-injected)
        self._interval: str | None = None
        self._interval_ms: int | None = None

        raw_cfg = feature_config or []
        normalized: list[Dict[str, Any]] = [_normalize_feature_item(dict(item)) for item in raw_cfg]

        names = [it["name"] for it in normalized]
        if len(names) != len(set(names)):
            dupes: list[str] = []
            seen: set[str] = set()
            for n in names:
                if n in seen and n not in dupes:
                    dupes.append(n)
                seen.add(n)
            raise ValueError(f"Duplicate feature names in feature_config: {dupes}")

        self.feature_config = normalized

        self.channels = [
            build_feature(
                item["type"],
                name=item["name"],
                symbol=item.get("symbol"),
                **item.get("params", {})
            )
            for item in self.feature_config
        ]

        log_debug(
            self._logger,
            "FeatureExtractor channels loaded",
            channels=[type(c).__name__ for c in self.channels]
        )

        # Each FeatureChannel.required_window() must return:
        #   dict[str, int]  e.g. {"ohlcv": 120, "orderbook": 5000}
        self.required_windows: Dict[str, int] = {}

        for ch in self.channels:
            w = ch.required_window()
            if not isinstance(w, dict):
                raise TypeError(
                    f"{type(ch).__name__}.required_window() must return dict[str, int], got {type(w)!r}"
                )
            for domain, n in w.items():
                if not isinstance(domain, str) or not isinstance(n, int) or n <= 0:
                    raise ValueError(
                        f"Invalid required_window entry from {type(ch).__name__}: {domain!r} -> {n!r}"
                    )
                self.required_windows[domain] = max(
                    self.required_windows.get(domain, 0), n
                )

        # Warmup windows are feature-state related, not data-related
    
        self.warmup_steps: int = max(min_warmup, 1)
        self._initialized = False
        self._last_timestamp = None
        self._last_output = {}
    def set_warmup_steps(self, n: int | None) -> None:
        
        if n is None:
            n = max(min_warmup, 1)
        elif not isinstance(n, int) or n <= 0:
            raise ValueError(f"Invalid warmup_steps: {n!r}")
        self.warmup_steps = n
    # ------------------------------------------------------------------
    # Strategy interval (engine-injected)
    # ------------------------------------------------------------------
    def set_interval(self, interval: str | None) -> None:
        """Inject strategy observation interval (e.g. '1m', '15m').

        This does NOT affect data handlers.
        It only informs feature semantics / aggregation policy.

        Runtime convention:
            - interval: str
            - interval_ms: int (derived)
        """
        self._interval = interval

        # derive ms when possible
        if isinstance(interval, str) and interval:
            ms = to_interval_ms(interval)
            self._interval_ms = int(ms) if ms is not None else None
        else:
            self._interval_ms = None

        for ch in self.channels:
            if hasattr(ch, "interval"):
                ch.interval = interval
            if hasattr(ch, "interval_ms"):
                try:
                    ch.interval_ms = self._interval_ms
                except Exception:
                    pass

    def set_interval_ms(self, interval_ms: int | None) -> None:
        """Inject strategy observation interval in epoch milliseconds.

        This is optional; if both interval and interval_ms are provided,
        interval_ms is treated as authoritative.
        """
        if interval_ms is None:
            self._interval_ms = None
        else:
            try:
                self._interval_ms = int(interval_ms)
            except Exception:
                raise ValueError(f"Invalid interval_ms: {interval_ms!r}")

        for ch in self.channels:
            if hasattr(ch, "interval_ms"):
                try:
                    ch.interval_ms = self._interval_ms
                except Exception:
                    pass

    # ----------------------------------------------------------------------
    # Full-window initialization
    # ----------------------------------------------------------------------
    def initialize(self) -> Dict[str, Any]:
        """
        Perform full-window initialization (historical warmup).
        Called on backtest startup or live cold-start.
        """
        # 1) Determine required warmup window across all channels
        

        # 2) Prefer OHLCV as primary warmup source, but don't assume it exists
        primary_handler: Optional[OHLCVDataHandler] = None

        if self.ohlcv_handlers:
            # Prefer primary symbol keyed handler when available
            primary_handler = self.ohlcv_handlers.get(getattr(self, "_primary_symbol", ""))
            if primary_handler is None:
                primary_handler = next(iter(self.ohlcv_handlers.values()))

        # Legacy helper for OHLCV-based features (optional)
        ohlcv_window = None
        if primary_handler is not None:
            n = self.required_windows.get("ohlcv")
            if isinstance(n, int) and n > 0 and hasattr(primary_handler, "window_df"):
                ohlcv_window = primary_handler.window_df(n) # window_df() specifically for OHLCVHandler

        timestamp_candidates: list[int] = []

        if hasattr(primary_handler, "last_timestamp"):
            assert primary_handler is not None
            v = primary_handler.last_timestamp()
            if v is not None:
                timestamp_candidates.append(int(v))

        # 3) Harvest timestamps from all other handler families
        def collect_ts(handlers: Dict[str, Any]) -> None:
            for h in handlers.values():
                if hasattr(h, "last_timestamp"):
                    v = h.last_timestamp()
                    if v is not None:
                        timestamp_candidates.append(int(v))

        collect_ts(self.orderbook_handlers)
        collect_ts(self.option_chain_handlers)
        collect_ts(self.iv_surface_handlers)
        collect_ts(self.sentiment_handlers)

        # 4) Initial logical time = max available timestamp, else 0
        timestamp0 = max(timestamp_candidates) if timestamp_candidates else 0

        context = {
            "timestamp": timestamp0,
            "interval": self._interval,
            "interval_ms": self._interval_ms,
            "data": {
                "ohlcv": self.ohlcv_handlers,
                "orderbook": self.orderbook_handlers,
                "option_chain": self.option_chain_handlers,
                "iv_surface": self.iv_surface_handlers,
                "sentiment": self.sentiment_handlers,
            },
            "required_windows": self.required_windows,
            "ohlcv_window": ohlcv_window,
        }

        # 5) Initialize all channels with the same context + warmup_window
        for ch in self.channels:
            ch.initialize(context, self.warmup_steps)

        # 6) Store initial output and internal state
        self._last_output = self.compute_output()
        self._initialized = True
        self._last_timestamp = timestamp0

        return self._last_output

    # ----------------------------------------------------------------------
    # Incremental update
    # ----------------------------------------------------------------------
    def update(self, timestamp: int | None = None) -> Dict[str, Any]:
        """
        Incremental update for new bar arrival.

        Parameters
        ----------
        timestamp : int | None
            Logical engine timestamp (epoch ms). If None, the extractor will infer
            the timestamp from available handlers (prefer OHLCV when present,
            otherwise fall back to the max last_timestamp across all families).
        """
        # If not initialized → perform warmup
        if not self._initialized:
            return self.initialize()

        if timestamp is None:
            raise RuntimeError(
                "FeatureExtractor.update() requires an explicit timestamp. "
                "Timestamp inference is owned by the engine/driver in v4."
            )

        assert timestamp is not None

        # ----------------------------------------------------------
        # 2) Anti-lookahead: if time hasn’t advanced, return cached
        # ----------------------------------------------------------
        if self._last_timestamp is not None and timestamp <= self._last_timestamp:
            return self._last_output

        # ----------------------------------------------------------
        # 3) Build context & update channels
        # ----------------------------------------------------------
        context = {
            "timestamp": timestamp,
            "interval": self._interval,
            "interval_ms": self._interval_ms,
            "data": {
                "ohlcv": self.ohlcv_handlers,
                "orderbook": self.orderbook_handlers,
                "option_chain": self.option_chain_handlers,
                "iv_surface": self.iv_surface_handlers,
                "sentiment": self.sentiment_handlers,
            },
            "required_windows": self.required_windows,
        }

        for ch in self.channels:
            ch.update(context)

        self._last_output = self.compute_output()
        self._last_timestamp = timestamp
        return self._last_output
    
    def warmup(self, *, anchor_ts: int) -> None:
        """
        Warm up feature internal state using preloaded handler caches.

        Semantics:
            - Feature-layer operation only.
            - Does NOT load data.
            - Does NOT advance time.
            - Repeatedly updates features at a fixed anchor timestamp
            to stabilize rolling statistics / internal buffers.
        """
        if self._initialized:
            # already warm
            return

        log_debug(
            self._logger,
            "FeatureExtractor warmup started",
            anchor_ts=anchor_ts,
            warmup_steps=self.warmup_steps,
        )

        # Build a minimal context shared across warmup steps
        context = {
            "timestamp": anchor_ts,
            "interval": self._interval,
            "interval_ms": self._interval_ms,
            "data": {
                "ohlcv": self.ohlcv_handlers,
                "orderbook": self.orderbook_handlers,
                "option_chain": self.option_chain_handlers,
                "iv_surface": self.iv_surface_handlers,
                "sentiment": self.sentiment_handlers,
            },
            "required_windows": self.required_windows,
        }

        # Let channels initialize themselves
        for ch in self.channels:
            ch.initialize(context, self.warmup_steps)

        # Feature warmup loop (state convergence)
        for _ in range(self.warmup_steps):
            for ch in self.channels:
                ch.update(context)

        self._initialized = True
        self._last_timestamp = anchor_ts
        self._last_output = self.compute_output()

        log_debug(self._logger, "FeatureExtractor warmup completed")

    # ----------------------------------------------------------------------
    # Output aggregator
    # ----------------------------------------------------------------------
    def compute_output(self) -> Dict[str, Any]:
        """
        Collect feature outputs.

        v4 contract:
            output = { feature.name : value }

        FeatureChannels may return either:
            - scalar
            - {name: value} (single-entry dict)
        """
        result: Dict[str, Any] = {}

        for ch in self.channels:
            name = ch.name
            value = ch.output()

            if isinstance(value, dict):
                if len(value) != 1:
                    raise ValueError(
                        f"Feature {name} returned multiple outputs; "
                        "v4 FeatureChannel must return a single value or single-key dict"
                    )
                value = next(iter(value.values()))

            result[name] = value

        return result

    # ----------------------------------------------------------------------
    # Thin wrapper (keeps old API)
    # ----------------------------------------------------------------------
    def compute(self) -> Dict[str, Any]:
        """
        Kept for backward compatibility.
        In v4 this performs an incremental update.
        """
        out = self.update()
        log_debug(self._logger, "FeatureExtractor computed features", keys=list(out.keys()))
        return out