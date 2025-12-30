import asyncio

import pytest

from quant_engine.exceptions.core import FatalError
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.runtime.realtime import RealtimeDriver


class _DummyPortfolio:
    def state(self):
        return {}


class _FailingEngine(EngineMode):
    def __init__(self):
        self.ohlcv_handlers = {}
        self.orderbook_handlers = {}
        self.option_chain_handlers = {}
        self.iv_surface_handlers = {}
        self.sentiment_handlers = {}
        self.trades_handlers = {}
        self.option_trades_handlers = {}
        self.feature_extractor = object()
        self.models = {}
        self.decision = object()
        self.risk_manager = object()
        self.execution_engine = object()
        self.portfolio = _DummyPortfolio()
        self.close_called = False

    def preload_data(self, anchor_ts=None):
        return None

    def warmup_features(self, anchor_ts=None):
        return None

    def align_to(self, ts):
        return None

    def step(self, ts):
        raise FatalError("boom")

    def close(self):
        self.close_called = True


class _TestDriver(RealtimeDriver):
    async def iter_timestamps(self):
        yield 1


@pytest.mark.asyncio
async def test_fatal_error_sets_stop_and_closes():
    engine = _FailingEngine()
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT")
    driver = _TestDriver(engine=engine, spec=spec)

    with pytest.raises(FatalError):
        await driver.run()

    assert driver.stop_event.is_set()
    assert engine.close_called
