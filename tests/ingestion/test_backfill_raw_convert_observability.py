from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from ingestion.option_trades.worker import OptionTradesWorker
from ingestion.orderbook.worker import OrderbookWorker
from ingestion.sentiment.worker import SentimentWorker
from ingestion.trades.worker import TradesWorker


class _FetchSource:
    def backfill(self, *, start_ts: int, end_ts: int):
        return [object()]


class _DummySource:
    pass


def _dummy_normalizer(*args, **kwargs):  # noqa: ANN002, ANN003
    return None


@pytest.mark.parametrize(
    "worker_factory",
    [
        lambda logger: TradesWorker(
            normalizer=SimpleNamespace(normalize=_dummy_normalizer),
            source=_DummySource(),
            fetch_source=_FetchSource(),
            symbol="BTCUSDT",
            logger=logger,
        ),
        lambda logger: OptionTradesWorker(
            normalizer=SimpleNamespace(normalize=_dummy_normalizer),
            source=_DummySource(),
            fetch_source=_FetchSource(),
            symbol="BTC",
            logger=logger,
        ),
        lambda logger: OrderbookWorker(
            normalizer=SimpleNamespace(normalize=_dummy_normalizer),
            source=_DummySource(),
            fetch_source=_FetchSource(),
            symbol="BTCUSDT",
            interval="1m",
            logger=logger,
        ),
        lambda logger: SentimentWorker(
            normalizer=SimpleNamespace(normalize=_dummy_normalizer, provider="x", symbol="x"),
            source=_DummySource(),
            fetch_source=_FetchSource(),
            interval="1m",
            logger=logger,
        ),
    ],
)
def test_backfill_logs_raw_convert_error(worker_factory, caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("test.backfill.raw_convert")
    logger.setLevel(logging.WARNING)
    worker = worker_factory(logger)

    with caplog.at_level(logging.WARNING, logger=logger.name):
        count = worker.backfill(start_ts=0, end_ts=10, anchor_ts=10, emit=None)

    assert count == 0
    assert any(rec.message == "ingestion.backfill.raw_convert_error" for rec in caplog.records)
