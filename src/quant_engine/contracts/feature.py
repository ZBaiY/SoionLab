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
    # Multi-symbol data access helpers (v4 unified interface)
    # ------------------------------------------------------------------
    def latest_bar(self, context: Dict[str, Any]):
        """
        Return the latest bar for this feature's symbol:
            context["ohlcv"][self.symbol]
        """
        ...

    def window(self, context: Dict[str, Any], n: int):
        """
        Return the past n bars for this feature's symbol.
        FeatureExtractor provides multi-symbol handlers via:
            context["ohlcv_handlers"]
        """
        ...

    def handler(self, context: Dict[str, Any]):
        """
        Return the RealTimeDataHandler for this feature's symbol.
        Typically:
            for h in context["ohlcv_handlers"]:
                if h.symbol == self.symbol: return h
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
    # Handler lookup
    # ------------------------------------------------------------------
    def handler(self, context: Dict[str, Any]):
        """
        Return the RealTimeDataHandler for this feature's symbol.
        Expects:
            context["ohlcv_handlers"] -> dict[str, RealTimeDataHandler]
        """
        handlers = context.get("ohlcv_handlers", {})
        if self.symbol in handlers:
            return handlers[self.symbol]
        raise KeyError(f"No handler found for symbol {self.symbol}")

    # ------------------------------------------------------------------
    # Latest bar access
    # ------------------------------------------------------------------
    def latest_bar(self, context: Dict[str, Any]):
        """
        Return the latest bar for this feature's symbol.
        Expects:
            context["ohlcv"] -> dict[symbol -> latest bar DataFrame]
        """
        h = self.handler(context)
        if hasattr(h, "latest_bar"):
            return h.latest_bar()
        raise AttributeError(f"Handler for {self.symbol} does not support latest_bar().")

    # ------------------------------------------------------------------
    # Window access
    # ------------------------------------------------------------------
    def window(self, context: Dict[str, Any], n: int):
        """
        Return a window of n bars for this feature's symbol.
        Obtained through its RealTimeDataHandler.
        """
        h = self.handler(context)
        if hasattr(h, "window_df"):
            return h.window_df(n)
        raise AttributeError(
            f"Handler for {self.symbol} does not support window_df()."
        )
