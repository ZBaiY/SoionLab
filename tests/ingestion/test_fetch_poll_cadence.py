from __future__ import annotations

from ingestion.option_chain.source import DeribitOptionChainRESTSource
from ingestion.orderbook.source import OrderbookRESTSource
from ingestion.sentiment.source import SentimentRESTSource


def test_option_chain_source_respects_explicit_poll_interval_with_interval() -> None:
    src = DeribitOptionChainRESTSource(
        currency="BTC",
        interval="15m",
        poll_interval_ms=5000,
    )
    try:
        assert src._poll_interval_ms == 5000
    finally:
        src._close_writers()


def test_sentiment_rest_source_respects_explicit_poll_interval_with_interval() -> None:
    src = SentimentRESTSource(
        fetch_fn=lambda: [],
        interval="15m",
        poll_interval_ms=5000,
    )
    assert src._poll_interval_ms == 5000


def test_orderbook_rest_source_respects_explicit_poll_interval_with_interval() -> None:
    src = OrderbookRESTSource(
        fetch_fn=lambda: [],
        interval="15m",
        poll_interval_ms=5000,
    )
    assert src._poll_interval_ms == 5000
