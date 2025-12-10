from typing import Protocol, Dict, Any, runtime_checkable

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
    def symbol(self) -> str | None:
        ...

    def initialize(self, context: Dict[str, Any], warmup_window: int | None = None) -> None:
        """
        Full-window initialization using full context:
            context = {
                "realtime_ohlcv": RealTimeDataHandler,
                "orderbook_realtime": RealTimeOrderbookHandler | None,
                "option_chain": OptionChainDataHandler | None,
                "sentiment": SentimentLoader | None,
            }
        """
        ...

    def update(self, context: Dict[str, Any]) -> None:
        """
        Incremental update using NEW data only, e.g.:
            context = {
                "ohlcv": new_bar_df (single-row),
                "historical": HistoricalDataHandler,
                "realtime": RealTimeDataHandler,
                "option_chain": latest option chain,
                "sentiment": latest sentiment,
            }
        """
        ...

    # ------------------------------------------------------------------
    # v4 Snapshot-Based Data Access (protocol-level)
    # ------------------------------------------------------------------
    def snapshot(self, context: Dict[str, Any], data_type: str, symbol: str | None = None):
        """
        Unified timestamp-aligned snapshot accessor.
        data_type ∈ {"ohlcv", "orderbook", "options", "iv_surface", "sentiment"}.
        If symbol is None, uses self.symbol.
        Implementations MUST retrieve snapshot via:
            handler.get_snapshot(context["ts"])
        """
        ...

    def window_any(self, context: Dict[str, Any], data_type: str, n: int, symbol: str | None = None):
        """
        Unified rolling window accessor for any handler implementing:
            window(ts, n)
        MUST enforce timestamp alignment via context["ts"].
        """
        ...

    def output(self) -> Dict[str, float]:
        """Return the current feature values."""
        ...

    def required_window(self) -> int:
        """
        Declare how many past bars are required for proper initialization.
        Default = 1 (only the latest bar).
        Feature implementations should override if they need longer windows.
        """
        return 1
    
"""
Base implementation for FeatureChannel helper utilities.
All concrete FeatureChannels should inherit from this class.
"""

from typing import Dict, Any


class FeatureChannelBase:
    """
    Provides unified v4 data-access helpers for multi-symbol feature computation.
    Concrete FeatureChannels MUST define:
        - self.symbol (str)
        - initialize(context)
        - update(context)
        - output()
    """
    symbol: str  # concrete subclasses must set this

    # ------------------------------------------------------------------
    # Handler lookup (generic)
    # ------------------------------------------------------------------
    def _get_handler(self, context: Dict[str, Any], data_type: str, symbol: str | None = None):
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
    def snapshot(self, context: Dict[str, Any], data_type: str, symbol: str | None = None):
        """
        Retrieve timestamp-aligned snapshot.
        Equivalent to:
            handler.get_snapshot(context["ts"])
        """
        h = self._get_handler(context, data_type, symbol)
        if not hasattr(h, "get_snapshot"):
            raise AttributeError(f"Handler for {data_type}:{symbol or self.symbol} has no get_snapshot().")
        return h.get_snapshot(context["ts"])

    # ------------------------------------------------------------------
    # Unified window accessor
    # ------------------------------------------------------------------
    def window_any(self, context: Dict[str, Any], data_type: str, n: int, symbol: str | None = None):
        """
        Retrieve timestamp-aligned rolling window of n items.
        Equivalent to:
            handler.window(context["ts"], n)
        """
        h = self._get_handler(context, data_type, symbol)
        if not hasattr(h, "window"):
            raise AttributeError(f"Handler for {data_type}:{symbol or self.symbol} has no window().")
        return h.window(context["ts"], n)
