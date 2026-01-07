from __future__ import annotations
from typing import Protocol, Dict, Any, cast, runtime_checkable
from collections.abc import Mapping, Iterable
import pandas as pd

from pyparsing import TypeVar, deque

from quant_engine.data.contracts.snapshot import Snapshot

SCHEMA_VERSION = 2

from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler
SnapT = TypeVar("SnapT", bound=Snapshot)


@runtime_checkable
class FeatureChannel(Protocol):
    """
    v4 Contract-Driven FeatureChannel Protocol

    This protocol separates:
        • initialize(window_df): full-window initialization (backtest / cold start)
        • update(new_bar): incremental update when a new bar arrives
        • output(): return the latest computed feature dict

    NOTE:
    - No __init__ defined here (implementation classes choose their own constructor)
    - symbol is exposed as a @property, not a required constructor argument
    """

    @property
    def name(self) -> str:
        """
        Stable feature identity used by downstream consumers (model / risk / decision).
        Must be unique within a strategy.
        """
        ...

    @property
    def symbol(self) -> str | None:
        ...

    @property
    def interval(self) -> str | None:
        """
        Strategy observation interval injected by the engine.
        Feature logic may branch on this value.
        """
        ...

    @interval.setter
    def interval(self, value: str | None) -> None:
        ...

    @property
    def interval_ms(self) -> int | None:
        """Strategy observation interval injected by the engine (epoch ms)."""
        ...

    @interval_ms.setter
    def interval_ms(self, value: int | None) -> None:
        ...

    def initialize(self, context: Dict[str, Any], warmup_window: int | None = None) -> None:
        """
        Full-window initialization using full context:
            context = {
                "timestamp": int,          # strategy observation timestamp (epoch ms)
                "interval": str,           # strategy observation interval (e.g. "1m", "15m")
                "interval_ms": int,        # strategy observation interval (epoch ms)
                "data": {
                    "ohlcv": {...},
                    "orderbook": {...},
                    "option_chain": {...},
                    "iv_surface": {...},
                    "sentiment": {...},
                }
            }
        """
        ...

    def update(self, context: Dict[str, Any]) -> None:
        """
        Incremental update using NEW data only, e.g.:
            context = {
                "timestamp": int,          # strategy observation timestamp (epoch ms)
                "interval": str,           # strategy observation interval (e.g. "1m", "15m")
                "interval_ms": int,        # strategy observation interval (epoch ms)
                "data": {
                    "ohlcv": {...},
                    "orderbook": {...},
                    "option_chain": {...},
                    "iv_surface": {...},
                    "sentiment": {...},
                }
            }
        """
        ...

    # ------------------------------------------------------------------
    # v4 Snapshot-Based Data Access (protocol-level)
    # ------------------------------------------------------------------
    def snapshot_dict(self, context: Dict[str, Any], data_type: str, symbol: str | None = None):
        """
        Unified timestamp-aligned snapshot accessor.
        data_type ∈ {"ohlcv", "orderbook", "options", "iv_surface", "sentiment"}.
        If symbol is None, uses self.symbol.
        Implementations MUST retrieve snapshot via:
            handler.get_snapshot(context["timestamp"])  # epoch ms int

        NOTE:
            Snapshot reflects the latest handler state with timestamp <= context["timestamp"] (epoch ms int).
            Handlers may update asynchronously between strategy steps.
        """
        ...

    def window_any(self, context: Dict[str, Any], data_type: str, n: int, symbol: str | None = None):
        """
        Unified rolling window accessor for any handler implementing:
            window(ts, n)
        MUST enforce timestamp alignment via context["timestamp"] (epoch ms int).
        """
        ...

    def output(self) -> float | int | None:
        """Return the current feature value."""
        ...

    def required_window(self) -> dict[str, int]:
        """
        Declare how many past bars are required for proper initialization.
        Default = 1 (only the latest bar).
        Feature implementations should override if they need longer windows.
        """
        return {}


class FeatureExtractorProto(Protocol):
    required_windows: dict[str, int]
    warmup_steps: int

    def warmup(self, *, anchor_ts: int) -> None:
        ...

    def update(self, timestamp: int | None = None) -> dict[str, Any]:
        ...

    def set_interval(self, interval: str | None) -> None:
        ...

    def set_interval_ms(self, interval_ms: int | None) -> None:
        ...
    
"""
Base implementation for FeatureChannel helper utilities.
All concrete FeatureChannels should inherit from this class.
"""



class FeatureChannelBase(FeatureChannel):
    """
    Provides unified v4 data-access helpers for multi-symbol feature computation.
    Concrete FeatureChannels MUST define:
        - self.symbol (str)
        - initialize(context)
        - update(context)
        - output()
    """

    @property
    def name(self) -> str:
        return self._name

    def __init__(self, *, name: str, symbol: str | None = None, **kwargs):
        self._name = name
        self._symbol = symbol
        self._interval: str | None = None
        self._interval_ms: int | None = None
        self._schema_version = SCHEMA_VERSION

    @property
    def symbol(self) -> str | None:
        return self._symbol

    @symbol.setter
    def symbol(self, value: str | None) -> None:
        self._symbol = value

    @property
    def interval(self) -> str | None:
        return self._interval

    @interval.setter
    def interval(self, value: str | None) -> None:
        self._interval = value

    @property
    def interval_ms(self) -> int | None:
        return self._interval_ms

    @interval_ms.setter
    def interval_ms(self, value: int | None) -> None:
        self._interval_ms = None if value is None else int(value)

    # ------------------------------------------------------------------
    # Handler lookup (generic)
    # ------------------------------------------------------------------
    def _get_handler(self, context: Dict[str, Any], data_type: str, symbol: str | None = None) -> RealTimeDataHandler:
        """
        Internal unified handler lookup.
        context must contain:
            context["data"][data_type][symbol]
        """
        data = context.get("data", {})
        handlers = data.get(data_type, {})
        
        key = symbol or self.symbol
        if key not in handlers:
            raise KeyError(f"No handler for {data_type}:{key}")
        
        return handlers[key]

    # ------------------------------------------------------------------
    # Snapshot / window normalization helpers
    # ------------------------------------------------------------------
    def _snapshot_to_dict(self, snapshot: Any) -> Dict[str, Any]:
        if snapshot is None:
            return {}
        to_dict = getattr(snapshot, "to_dict", None)
        if callable(to_dict):
            try:
                out = to_dict()
                if isinstance(out, Mapping):
                    return dict(out)
            except Exception:
                pass
        if isinstance(snapshot, Mapping):
            return dict(snapshot)
        return {}

    def _window_to_dicts(self, window: Any) -> list[Dict[str, Any]]:
        if window is None:
            return []
        if pd is not None and isinstance(window, pd.DataFrame):
            return cast(list[Dict[str, Any]], window.to_dict(orient="records"))
        if pd is not None and isinstance(window, pd.Series):
            return [window.to_dict()]
        if isinstance(window, Mapping):
            return [dict(window)]
        if isinstance(window, Iterable) and not isinstance(window, (str, bytes)):
            return [self._snapshot_to_dict(snap) for snap in window if snap is not None]
        return []

    def _resolve_warmup_anchor_ts(self, context: Dict[str, Any]) -> int | None:
        anchor = context.get("anchor_ts")
        if anchor is None:
            anchor = context.get("timestamp")
        try:
            return None if anchor is None else int(anchor)
        except Exception:
            return None

    def _resolve_warmup_step_ms(
        self,
        context: Dict[str, Any],
        *,
        data_type: str | None = None,
        symbol: str | None = None,
    ) -> int | None:
        step_ms = context.get("engine_interval_ms")
        if isinstance(step_ms, int) and step_ms > 0:
            return int(step_ms)
        if isinstance(self.interval_ms, int) and self.interval_ms > 0:
            return int(self.interval_ms)
        data = context.get("data", {})
        if data_type:
            handlers = data.get(data_type, {})
            key = symbol or self.symbol
            h = handlers.get(key) if isinstance(handlers, Mapping) else None
            v = getattr(h, "interval_ms", None)
            if isinstance(v, int) and v > 0:
                return int(v)
        if isinstance(data, Mapping):
            for handlers in data.values():
                if not isinstance(handlers, Mapping) or not handlers:
                    continue
                key = symbol or self.symbol
                h = handlers.get(key) or next(iter(handlers.values()))
                v = getattr(h, "interval_ms", None)
                if isinstance(v, int) and v > 0:
                    return int(v)
        return None

    def _warmup_by_update(
        self,
        context: Dict[str, Any],
        warmup_steps: int | None,
        *,
        data_type: str | None = None,
        symbol: str | None = None,
    ) -> None:
        steps = int(warmup_steps) if warmup_steps is not None else 1
        if steps <= 0:
            steps = 1
        anchor_ts = self._resolve_warmup_anchor_ts(context)
        if anchor_ts is None:
            self.update(context)
            return
        step_ms = self._resolve_warmup_step_ms(context, data_type=data_type, symbol=symbol)
        if step_ms is None or step_ms <= 0:
            ctx = dict(context)
            ctx["timestamp"] = int(anchor_ts)
            self.update(ctx)
            return
        for i in range(steps):
            ts_i = int(anchor_ts) - (steps - 1 - i) * int(step_ms)
            ctx = dict(context)
            ctx["timestamp"] = int(ts_i)
            self.update(ctx)

    # ------------------------------------------------------------------
    # Unified snapshot accessor
    # ------------------------------------------------------------------
    def snapshot_dict(self, context: Dict[str, Any], data_type: str, symbol: str | None = None) -> Dict[str, Any]:
        """
        Retrieve timestamp-aligned snapshot.
        Equivalent to:
            handler.get_snapshot(context["timestamp"])

        NOTE:
            Snapshot reflects the latest handler state with timestamp <= context["timestamp"] (epoch ms int).
            Handlers may update asynchronously between strategy steps.
        """
        h = self._get_handler(context, data_type, symbol)
        assert isinstance(h, RealTimeDataHandler), f"Handler for {data_type}:{symbol or self.symbol} is not a RealtimeDataHandler."
        if not hasattr(h, "get_snapshot"):
            raise AttributeError(f"Handler for {data_type}:{symbol or self.symbol} has no get_snapshot().")
        snap = h.get_snapshot(context["timestamp"])
        if snap is None:
            return {}
        return self._snapshot_to_dict(snap)

    # ------------------------------------------------------------------
    # Unified window accessor
    # ------------------------------------------------------------------
    def window_any(self, context: Dict[str, Any], data_type: str, n: int, symbol: str | None = None) -> list[Snapshot]:
        """
        Retrieve timestamp-aligned rolling window of n items.
        Equivalent to:
            handler.window(context["timestamp"], n)
        """
        h = self._get_handler(context, data_type, symbol)
        if not hasattr(h, "window"):
            raise AttributeError(f"Handler for {data_type}:{symbol or self.symbol} has no window().")
        return h.window(context["timestamp"], n)

    def window_any_dicts(
        self,
        context: Dict[str, Any],
        data_type: str,
        n: int,
        symbol: str | None = None,
    ) -> list[Dict[str, Any]]:
        window = self.window_any(context, data_type, n, symbol)
        return self._window_to_dicts(window)

    def window_any_df(
        self,
        context: Dict[str, Any],
        data_type: str,
        n: int,
        symbol: str | None = None,
    ) -> Any:
        h = self._get_handler(context, data_type, symbol)
        if not hasattr(h, "window"):
            raise AttributeError(f"Handler for {data_type}:{symbol or self.symbol} has no window().")
        window = h.window(context["timestamp"], n)
        if pd is not None and isinstance(window, pd.DataFrame):
            return window if len(window) >= int(n) else None
        rows = self._window_to_dicts(window)
        if len(rows) < int(n):
            return None
        if pd is None:
            return rows
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Strategy-interval helper (optional)
    # ------------------------------------------------------------------
    def strategy_window(self, context: Dict[str, Any], data_type: str, n: int, symbol: str | None = None):
        """
        Retrieve rolling window aligned to the *strategy observation interval*.

        This is a semantic helper, not a data-layer primitive.
        Feature implementations may choose to:
            - aggregate high-frequency data into one strategy-step value
            - or ignore this helper and use snapshot/window_any directly
        """
        return self.window_any(context, data_type, n, symbol)

    def market_status(self, context: Dict[str, Any], data_type: str = "ohlcv", symbol: str | None = None) -> str | None:
        h = self._get_handler(context, data_type, symbol)
        if not hasattr(h, "get_snapshot"):
            return None
        snap = h.get_snapshot(context["timestamp"])
        if snap is None:
            return None
        market = getattr(snap, "market", None)
        status = getattr(market, "status", None)
        if status is None:
            return None
        return str(status)

    def market_is_active(self, context: Dict[str, Any], data_type: str = "ohlcv", symbol: str | None = None) -> bool:
        status = self.market_status(context, data_type=data_type, symbol=symbol)
        if status is None:
            return True
        return str(status).lower() == "open"
