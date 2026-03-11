from __future__ import annotations

import logging

import pytest

from quant_engine.contracts.exchange_account import AccountState, AssetBalance, SymbolConstraints
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
async def test_iter_timestamps_aligns_off_grid_realtime_start(
    deterministic_clock,
    noop_sleep,
) -> None:
    deterministic_clock([1.234, 1.234, 1.234])
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT", timestamp=1234)
    driver = RealtimeDriver(engine=_Engine(primary=_Primary(interval_ms=1000, seq=[999])), spec=spec)  # type: ignore[arg-type]

    timestamps = driver.iter_timestamps()
    first = await timestamps.__anext__()
    second = await timestamps.__anext__()
    await timestamps.aclose()

    assert first == 2000
    assert second == 3000


@pytest.mark.asyncio
async def test_iter_timestamps_applies_post_close_delay(
    monkeypatch: pytest.MonkeyPatch,
    deterministic_clock,
) -> None:
    deterministic_clock([1.234, 1.234])
    slept: list[float] = []

    async def _sleep(delay_s: float) -> None:
        slept.append(float(delay_s))
        return None

    async def _no_lag_monitor(**kwargs):
        return None

    monkeypatch.setattr("quant_engine.runtime.realtime.asyncio.sleep", _sleep)
    monkeypatch.setattr("quant_engine.runtime.realtime.loop_lag_monitor", _no_lag_monitor)

    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT", timestamp=1234)
    driver = RealtimeDriver(
        engine=_Engine(primary=_Primary(interval_ms=1000, seq=[999])),  # type: ignore[arg-type]
        spec=spec,
        step_delay_ms=200,
    )

    timestamps = driver.iter_timestamps()
    first = await timestamps.__anext__()
    await timestamps.aclose()

    assert first == 2000
    assert slept
    assert slept[0] == pytest.approx((2200 - 1234) / 1000.0)


class _SyncingPortfolio:
    def __init__(self) -> None:
        self.symbol = "BTCUSDT"
        self.cash = 1.0
        self.position_qty = 0.0
        self.sync_calls = 0

    def sync_from_exchange(self, account_state: AccountState, *, symbol: str | None = None, quote_asset: str | None = None) -> None:
        self.sync_calls += 1
        if quote_asset is not None:
            self.cash = float(account_state.balances[str(quote_asset)].free)
        key = str(symbol or self.symbol)
        self.position_qty = float(account_state.positions.get(key, 0.0) or 0.0)

    def state(self) -> PortfolioState:
        return PortfolioState(
            {
                "cash": self.cash,
                "position": self.position_qty,
                "position_qty": self.position_qty,
            }
        )


class _StaticAccountAdapter:
    def get_account_state(self) -> AccountState:
        return AccountState(
            balances={"USDT": AssetBalance(free=25.0, locked=0.0)},
            positions={"BTCUSDT": 0.25},
            timestamp=1_700_000_000_000,
        )

    def get_symbol_constraints(self, symbol: str) -> SymbolConstraints:
        return SymbolConstraints(
            step_size=0.01,
            min_qty=0.01,
            min_notional=10.0,
            base_asset="BTC",
            quote_asset="USDT",
        )


class _ExchangeSyncedEngine(_Engine):
    def __init__(self, primary: _Primary):
        super().__init__(primary=primary)
        self.symbol = "BTCUSDT"
        self.portfolio = _SyncingPortfolio()
        self.exchange_account_adapter = _StaticAccountAdapter()
        self.exchange_account_symbol = "BTCUSDT"
        self.exchange_symbol_constraints = SymbolConstraints(
            step_size=0.01,
            min_qty=0.01,
            min_notional=10.0,
            base_asset="BTC",
            quote_asset="USDT",
        )

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
            portfolio=self.portfolio.state(),
        )


class _TraceRecordingExchangeEngine(_ExchangeSyncedEngine):
    def __init__(self, primary: _Primary):
        super().__init__(primary=primary)
        self.pending_trace_cash: float | None = None
        self.flushed_trace_cash: float | None = None
        self.flush_calls = 0
        self._pending_step_trace: dict[str, object] | None = None

    def step(self, *, ts: int) -> EngineSnapshot:
        snapshot = super().step(ts=ts)
        self.pending_trace_cash = float(snapshot.portfolio.to_dict()["cash"])
        self._pending_step_trace = {"step_ts": int(ts)}
        return snapshot

    def flush_pending_step_trace(self, *, snapshot: EngineSnapshot | None = None) -> None:
        self.flush_calls += 1
        assert snapshot is not None
        self.flushed_trace_cash = float(snapshot.portfolio.to_dict()["cash"])
        self._pending_step_trace = None


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
async def test_step_reconciles_portfolio_from_exchange_account(
    deterministic_clock,
    noop_sleep,
    inline_thread_calls,
) -> None:
    deterministic_clock([1_700_000_000.0])
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT", timestamp=1000)
    engine = _ExchangeSyncedEngine(primary=_Primary(interval_ms=1000, seq=[999, 1999]))
    driver = _FiniteDriver(engine=engine, spec=spec, timestamps=[1000])  # type: ignore[arg-type]

    await driver.run()

    assert engine.step_calls == 1
    assert engine.portfolio.sync_calls == 1
    snap = driver.snapshots[-1]
    assert snap.portfolio.to_dict()["cash"] == 25.0
    assert snap.portfolio.to_dict()["position_qty"] == 0.25


@pytest.mark.asyncio
async def test_step_trace_flushes_reconciled_portfolio(
    deterministic_clock,
    noop_sleep,
    inline_thread_calls,
) -> None:
    deterministic_clock([1_700_000_000.0])
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT", timestamp=1000)
    engine = _TraceRecordingExchangeEngine(primary=_Primary(interval_ms=1000, seq=[999, 1999]))
    driver = _FiniteDriver(engine=engine, spec=spec, timestamps=[1000])  # type: ignore[arg-type]

    await driver.run()

    assert engine.pending_trace_cash == 1.0
    assert engine.flushed_trace_cash == 25.0
    assert engine.flush_calls == 1


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


@pytest.mark.asyncio
async def test_mainloop_recovery_proceeds_to_step(
    monkeypatch: pytest.MonkeyPatch,
    deterministic_clock,
    noop_sleep,
    inline_thread_calls,
) -> None:
    deterministic_clock([1_700_000_000.0])
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT", timestamp=1000)
    engine = _Engine(primary=_Primary(interval_ms=1000, seq=[0, 999]))
    driver = _FiniteDriver(engine=engine, spec=spec, timestamps=[1000])  # type: ignore[arg-type]

    monkeypatch.setattr(RealtimeDriver, "_recover_mainloop_ohlcv_gap", lambda self, **kwargs: (True, []))

    await driver.run()
    assert engine.step_calls == 1


@pytest.mark.asyncio
async def test_mainloop_recovery_fatal_labels_maintenance(
    monkeypatch: pytest.MonkeyPatch,
    deterministic_clock,
    noop_sleep,
    inline_thread_calls,
) -> None:
    deterministic_clock([1_700_000_000.0])
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT", timestamp=1000)
    engine = _Engine(primary=_Primary(interval_ms=1000, seq=[0]))
    market = type("Market", (), {"status": "closed", "gap_type": "expected_closed", "calendar": "24x7"})()
    engine.primary.market = market # type: ignore[attr-defined]
    driver = _FiniteDriver(engine=engine, spec=spec, timestamps=[1000])  # type: ignore[arg-type]

    monkeypatch.setattr(
        RealtimeDriver,
        "_recover_mainloop_ohlcv_gap",
        lambda self, **kwargs: (False, ["ohlcv:BTCUSDT"]),
    )

    with pytest.raises(FatalError, match="MAINTENANCE_STOP: mainloop catch-up failed after 3 rounds"):
        await driver.run()


@pytest.mark.asyncio
async def test_reconciliation_success_logged(
    caplog: pytest.LogCaptureFixture,
    deterministic_clock,
    noop_sleep,
    inline_thread_calls,
) -> None:
    deterministic_clock([1_700_000_000.0])
    spec = EngineSpec.from_interval(mode=EngineMode.REALTIME, interval="1s", symbol="BTCUSDT", timestamp=1000)
    engine = _ExchangeSyncedEngine(primary=_Primary(interval_ms=1000, seq=[999, 1999]))
    driver = _FiniteDriver(engine=engine, spec=spec, timestamps=[1000])  # type: ignore[arg-type]

    with caplog.at_level(logging.INFO):
        await driver.run()

    recon_logs = [r for r in caplog.records if r.message == "runtime.exchange_account.reconciled"]
    assert len(recon_logs) == 1
    ctx = getattr(recon_logs[0], "context", {})
    assert ctx.get("symbol") == "BTCUSDT"
    assert ctx.get("exchange_quote_free") == 25.0
    assert ctx.get("exchange_base_qty") == 0.25
