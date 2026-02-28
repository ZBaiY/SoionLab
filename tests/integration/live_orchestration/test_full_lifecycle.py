from __future__ import annotations

import logging

import pandas as pd
import pytest

from quant_engine.contracts.portfolio import PortfolioState
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.runtime.realtime import RealtimeDriver
from quant_engine.runtime.snapshot import EngineSnapshot


class _FiniteDriver(RealtimeDriver):
    def __init__(self, *args, timestamps: list[int], **kwargs):
        super().__init__(*args, **kwargs)
        self._timestamps = list(timestamps)

    async def iter_timestamps(self):
        for ts in self._timestamps:
            yield int(ts)


@pytest.mark.asyncio
async def test_full_lifecycle_mock(
    caplog: pytest.LogCaptureFixture,
    make_engine,
    deterministic_clock,
    noop_sleep,
    inline_thread_calls,
    fresh_ohlcv_window,
) -> None:
    engine = make_engine(mode=EngineMode.MOCK, required_windows={"ohlcv": 2, "option_chain": 5}, warmup_steps=2)
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1m", symbol="BTCUSDT", timestamp=1_704_067_320_000)
    engine.spec = spec

    ohlcv = engine.ohlcv_handlers["BTCUSDT"]
    option_chain = engine.option_chain_handlers["BTCUSDT"]
    bootstrap_calls: dict[str, list[int]] = {"ohlcv": [], "option_chain": []}

    ohlcv.bootstrap = (lambda *, anchor_ts=None, lookback=None: bootstrap_calls["ohlcv"].append(int(lookback)))  # type: ignore[method-assign]
    option_chain.bootstrap = (lambda *, anchor_ts=None, lookback=None: bootstrap_calls["option_chain"].append(int(lookback)))  # type: ignore[method-assign]

    ohlcv.window = (lambda ts, n: fresh_ohlcv_window(int(ts), int(n), int(ohlcv.interval_ms)))  # type: ignore[method-assign]
    option_chain.window = (lambda ts, n: [])  # type: ignore[method-assign]
    ohlcv.last_timestamp = (lambda: 1_704_067_440_000)  # type: ignore[method-assign]

    step_calls = {"n": 0}

    def _step(*, ts: int):
        step_calls["n"] += 1
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

    engine.step = _step  # type: ignore[method-assign]

    deterministic_clock([1_700_000_000.0])
    driver = _FiniteDriver(engine=engine, spec=spec, timestamps=[1_704_067_320_000, 1_704_067_380_000])  # type: ignore[arg-type]

    with caplog.at_level(logging.WARNING):
        await driver.run()

    assert bootstrap_calls["ohlcv"] and bootstrap_calls["option_chain"]
    assert step_calls["n"] == 2
    assert len(driver.snapshots) == 2
    assert any("engine.warmup.soft_domain_insufficient" in rec.getMessage() for rec in caplog.records)
    assert not any("runtime.step.data_not_ready" in rec.getMessage() for rec in caplog.records)
