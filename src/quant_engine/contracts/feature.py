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

    def initialize(self, context: Dict[str, Any]) -> None:
        """
        Full-window initialization using full context:
            context = {
                "ohlcv": full_window_df,
                "historical": HistoricalDataHandler,
                "realtime": RealTimeDataHandler,
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