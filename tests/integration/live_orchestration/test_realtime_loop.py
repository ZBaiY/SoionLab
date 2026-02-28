from __future__ import annotations

import logging

import pytest

from quant_engine.contracts.portfolio import PortfolioState
from quant_engine.exceptions.core import FatalError
from quant_engine.runtime.driver import BaseDriver
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.runtime.realtime import RealtimeDriver
from quant_engine.runtime.snapshot import EngineSnapshot


class _Primary:
    def __init__(self, *, interval_ms: int, seq: list[int | None]):
        self.interval_ms = int(interval_ms)
        self._seq = list(seq)
        self._idx = 0

    def last_timestamp(self):
        i = self._idx
        self._idx += 1
        if i >= len(self._seq):
            return self._seq[-1]
        return self._seq[i]


class _Engine:
    def __init__(self, primary: _Primary):
        self.primary = primary
        self.step_calls = 0

        class _Feat:
            def update(self, timestamp: int | None = None):
                return {}

        self.feature_extractor = _Feat()

    def bootstrap(self, *, anchor_ts: int | None = None):
        return None

    def warmup_features(self, *, anchor_ts: int | None = None):
        return None

    def align_to(self, ts: int):
        return None

    def _get_primary_ohlcv_handler(self):
        return self.primary

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


class _FiniteDriver(RealtimeDriver):
    def __init__(self, *args, timestamps: list[int], **kwargs):
        super().__init__(*args, **kwargs)
        self._timestamps = list(timestamps)

    async def iter_timestamps(self):
        for ts in self._timestamps:
            yield int(ts)


@pytest.mark.asyncio
async def test_readiness_gate_skips_stale(
    caplog: pytest.LogCaptureFixture,
    deterministic_clock,
    noop_sleep,
    inline_thread_calls,
) -> None:
    deterministic_clock([1_700_000_000.0, 1_700_000_001.0])
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT", timestamp=1000)
    engine = _Engine(primary=_Primary(interval_ms=1000, seq=[None, 0, 0]))
    driver = _FiniteDriver(engine=engine, spec=spec, timestamps=[1000, 2000])  # type: ignore[arg-type]
    with caplog.at_level(logging.WARNING):
        await driver.run()
    assert engine.step_calls == 0
    assert any("runtime.step.data_not_ready" in rec.getMessage() for rec in caplog.records)


@pytest.mark.asyncio
async def test_step_runs_when_fresh(deterministic_clock, noop_sleep, inline_thread_calls) -> None:
    deterministic_clock([1_700_000_000.0])
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT", timestamp=1000)
    engine = _Engine(primary=_Primary(interval_ms=1000, seq=[999, 1999]))
    driver = _FiniteDriver(engine=engine, spec=spec, timestamps=[1000])  # type: ignore[arg-type]
    await driver.run()
    assert engine.step_calls == 1
    assert len(driver.snapshots) == 1


@pytest.mark.asyncio
async def test_catchup_fatal_after_max_rounds(
    monkeypatch: pytest.MonkeyPatch,
    deterministic_clock,
    noop_sleep,
    inline_thread_calls,
) -> None:
    deterministic_clock([1_700_000_000.0 + float(i) for i in range(200)])
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT")
    engine = _Engine(primary=_Primary(interval_ms=1000, seq=[0, 0, 0, 0]))
    driver = _FiniteDriver(engine=engine, spec=spec, timestamps=[1000])  # type: ignore[arg-type]

    monkeypatch.setattr(BaseDriver, "_collect_gap_labels", lambda self, *, target_ts: ["ohlcv:BTCUSDT"])

    with pytest.raises(FatalError, match="Catch-up failed after 3 rounds"):
        await driver.run()


@pytest.mark.asyncio
async def test_catchup_succeeds_gaps_clear_round2(
    monkeypatch: pytest.MonkeyPatch,
    deterministic_clock,
    noop_sleep,
    inline_thread_calls,
) -> None:
    deterministic_clock([1_700_000_000.0 + float(i) for i in range(200)])
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT")
    engine = _Engine(primary=_Primary(interval_ms=1000, seq=[5_000_000_000, 5_000_000_000]))
    driver = _FiniteDriver(engine=engine, spec=spec, timestamps=[1000])  # type: ignore[arg-type]

    state = {"n": 0}

    def _collect_gaps(self, *, target_ts: int):
        state["n"] += 1
        return ["ohlcv:BTCUSDT"] if state["n"] == 1 else []

    monkeypatch.setattr(BaseDriver, "_collect_gap_labels", _collect_gaps)

    await driver.run()
    assert engine.step_calls == 1
