from typing import Protocol, Dict, Any, runtime_checkable

SCHEMA_VERSION = 2

from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler

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
    def _get_handler(self, context: Dict[str, Any], data_type: str, symbol: str | None = None) -> Any:
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
        return snap.to_dict()

    # ------------------------------------------------------------------
    # Unified window accessor
    # ------------------------------------------------------------------
    def window_any(self, context: Dict[str, Any], data_type: str, n: int, symbol: str | None = None):
        """
        Retrieve timestamp-aligned rolling window of n items.
        Equivalent to:
            handler.window(context["timestamp"], n)
        """
        h = self._get_handler(context, data_type, symbol)
        if not hasattr(h, "window"):
            raise AttributeError(f"Handler for {data_type}:{symbol or self.symbol} has no window().")
        return h.window(context["timestamp"], n)

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
