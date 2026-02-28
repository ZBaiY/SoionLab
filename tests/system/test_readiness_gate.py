from __future__ import annotations

import logging

import pytest

from quant_engine.contracts.portfolio import PortfolioState
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.runtime.realtime import RealtimeDriver
from quant_engine.runtime.snapshot import EngineSnapshot


class _PrimaryOHLCV:
    interval_ms = 1000

    def __init__(self) -> None:
        self._seq = [None, None, None, 2999]
        self._idx = 0

    def last_timestamp(self):
        v = self._seq[self._idx] if self._idx < len(self._seq) else self._seq[-1]
        self._idx += 1
        return v


class _Engine:
    def __init__(self) -> None:
        self.step_calls = 0
        self._h = _PrimaryOHLCV()

    def bootstrap(self, *, anchor_ts: int | None = None) -> None:
        return None

    def warmup_features(self, *, anchor_ts: int | None = None) -> None:
        return None

    def align_to(self, ts: int) -> None:
        return None

    def _get_primary_ohlcv_handler(self):
        return self._h

    def step(self, *, ts: int) -> EngineSnapshot:
        self.step_calls += 1
        return EngineSnapshot(
            timestamp=int(ts),
            mode=EngineMode.REALTIME,
            features={},
            model_outputs={},
            decision_score=None,
            target_position=None,
            fills=[],
            portfolio=PortfolioState({"symbol": "BTCUSDT"}),
        )

    def iter_shutdown_objects(self):
        return []


class _ThreeStepDriver(RealtimeDriver):
    async def iter_timestamps(self):
        yield 1000
        yield 2000
        yield 3000


@pytest.mark.asyncio
async def test_realtime_readiness_gate_skips_stale_steps(caplog: pytest.LogCaptureFixture) -> None:
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT", timestamp=1000)
    engine = _Engine()
    driver = _ThreeStepDriver(engine=engine, spec=spec)  # type: ignore[arg-type]
    with caplog.at_level(logging.WARNING):
        await driver.run()
    assert engine.step_calls == 1
    warn_count = sum(1 for rec in caplog.records if "runtime.step.data_not_ready" in rec.getMessage())
    assert warn_count == 2
    prewarm_warn_count = sum(1 for rec in caplog.records if "runtime.prewarm.cleaned_stale" in rec.getMessage())
    assert prewarm_warn_count == 1
