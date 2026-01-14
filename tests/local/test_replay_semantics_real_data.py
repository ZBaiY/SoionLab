from __future__ import annotations

# Requires local cleaned dataset; skipped in CI.

from datetime import datetime, timezone
from pathlib import Path

import asyncio
import pytest

from ingestion.contracts.tick import IngestionTick, _coerce_epoch_ms, _to_interval_ms
from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer
from ingestion.ohlcv.source import OHLCVFileSource
from ingestion.ohlcv.worker import OHLCVWorker
from ingestion.option_chain.normalize import DeribitOptionChainNormalizer
from ingestion.option_chain.source import OptionChainFileSource
from ingestion.option_chain.worker import OptionChainWorker
from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler


# Helper exception for local replay test control-flow
class _StopReplay(Exception):
    """Internal control-flow exception to stop local replay tests early."""


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

# START_MS_OH = _to_ms(datetime(2023, 12, 1, 22, 0, 0, tzinfo=timezone.utc))
# END_MS_OH = _to_ms(datetime(2024, 2, 28, 3, 0, 0, tzinfo=timezone.utc))

START_MS = _to_ms(datetime(2025, 12, 29, 22, 0, 0, tzinfo=timezone.utc))
END_MS = _to_ms(datetime(2025, 12, 30, 3, 0, 0, tzinfo=timezone.utc))
START_MS_OH = _to_ms(datetime(2025, 12, 29, 22, 0, 0, tzinfo=timezone.utc))
END_MS_OH = _to_ms(datetime(2025, 12, 30, 3, 0, 0, tzinfo=timezone.utc))


@pytest.mark.local
@pytest.mark.asyncio
async def test_replay_semantics_ohlcv_15m_btcusdt(monkeypatch: pytest.MonkeyPatch) -> None:
    path = Path("data/cleaned/ohlcv/BTCUSDT/15m/2025.parquet")
    if not path.exists():
        pytest.skip(f"Missing local data file: {path}")

    source = OHLCVFileSource(
        root=Path("data/cleaned/ohlcv"),
        symbol="BTCUSDT",
        interval="15m",
        start_ts=START_MS_OH,
        end_ts=END_MS_OH,
    )

    normalizer = BinanceOHLCVNormalizer(symbol="BTCUSDT")
    worker = OHLCVWorker(
        source=source,
        normalizer=normalizer,
        symbol="BTCUSDT",
        interval="15m",
        interval_ms=int(_to_interval_ms("15m") or 900_000),
        poll_interval_ms=1,
    )

    orig_sleep = asyncio.sleep
    async def fast_sleep(_seconds: float) -> None:
        await orig_sleep(0)


    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    emitted: list[IngestionTick] = []

    count = 0

    async def emit(tick: IngestionTick) -> None:
        nonlocal count
        emitted.append(tick)
        count += 1
        if count >= 500:
            task.cancel()

    task = asyncio.create_task(worker.run(emit))
    try:
        await task
    except asyncio.CancelledError:
        pass

    windowed = [t for t in emitted if START_MS_OH <= int(t.data_ts) <= END_MS_OH]
    if len(windowed) < 5:
        pytest.skip("Not enough OHLCV bars in window for test")
    emitted = windowed
    assert all(isinstance(t, IngestionTick) for t in emitted)
    assert all(t.domain == "ohlcv" for t in emitted)
    assert all(t.symbol == "BTCUSDT" for t in emitted)
    data_ts = [t.data_ts for t in emitted]
    assert data_ts == sorted(data_ts)
    assert all("open" in t.payload and "high" in t.payload and "low" in t.payload and "close" in t.payload for t in emitted)

    handler = OHLCVDataHandler("BTCUSDT", interval="15m", cache={"maxlen": 10_000})
    anchors = [data_ts[0], data_ts[len(data_ts) // 2], data_ts[-1]]
    for anchor_ts in anchors:
        handler.align_to(anchor_ts)
        for tick in emitted:
            if tick.data_ts <= anchor_ts:
                handler.on_new_tick(tick)
        assert handler.last_timestamp() is None or handler.last_timestamp() <= anchor_ts # type: ignore
        snap = handler.get_snapshot(anchor_ts)
        assert snap is None or snap.data_ts <= anchor_ts
        dfw = handler.window(anchor_ts, n=10)
        if not dfw.empty and "data_ts" in dfw.columns:
            assert int(dfw["data_ts"].max()) <= anchor_ts


@pytest.mark.local
@pytest.mark.asyncio
async def test_replay_semantics_option_chain_1m_btc_parallel_stability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = Path("data/cleaned/option_chain/BTC/1m/2025")
    p1 = base / "2025_12_29.parquet"
    p2 = base / "2025_12_30.parquet"
    if not p1.exists() or not p2.exists():
        pytest.skip(f"Missing local data files: {p1} or {p2}")

    option_source = OptionChainFileSource(
        root=Path("data/cleaned/option_chain"),
        asset="BTC",
        interval="1m",
        start_ts=START_MS,
        end_ts=END_MS,
    )

    ohlcv_path = Path("data/cleaned/ohlcv/BTCUSDT/15m/2025.parquet")
    if not ohlcv_path.exists():
        pytest.skip(f"Missing local data file: {ohlcv_path}")
    ohlcv_source = OHLCVFileSource(
        root=Path("data/cleaned/ohlcv"),
        symbol="BTCUSDT",
        interval="15m",
        start_ts=START_MS_OH,
        end_ts=END_MS_OH,
    )

    option_normalizer = DeribitOptionChainNormalizer(symbol="BTC")
    option_worker = OptionChainWorker(
        source=option_source,
        normalizer=option_normalizer,
        symbol="BTC",
        interval="1m",
        interval_ms=int(_to_interval_ms("1m") or 60_000),
        poll_interval_ms=0,
    )

    ohlcv_normalizer = BinanceOHLCVNormalizer(symbol="BTCUSDT")
    ohlcv_worker = OHLCVWorker(
        source=ohlcv_source,
        normalizer=ohlcv_normalizer,
        symbol="BTCUSDT",
        interval="15m",
        interval_ms=int(_to_interval_ms("15m") or 900_000),
        poll_interval_ms=1,
    )

    orig_sleep = asyncio.sleep
    async def fast_sleep(_seconds: float) -> None:
        await orig_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    option_emitted: list[IngestionTick] = []
    ohlcv_emitted: list[IngestionTick] = []

    option_count = 0
    ohlcv_count = 0

    async def emit_option(tick: IngestionTick) -> None:
        nonlocal option_count
        option_emitted.append(tick)
        option_count += 1
        if option_count >= 500:
            option_task.cancel()
            ohlcv_task.cancel()

    async def emit_ohlcv(tick: IngestionTick) -> None:
        nonlocal ohlcv_count
        ohlcv_emitted.append(tick)
        ohlcv_count += 1
        if ohlcv_count >= 500:
            option_task.cancel()
            ohlcv_task.cancel()

    option_task = asyncio.create_task(option_worker.run(emit_option))
    ohlcv_task = asyncio.create_task(ohlcv_worker.run(emit_ohlcv))
    await asyncio.gather(option_task, ohlcv_task, return_exceptions=True)

    option_emitted = [t for t in option_emitted if START_MS <= int(t.data_ts) <= END_MS]
    ohlcv_emitted = [t for t in ohlcv_emitted if START_MS_OH <= int(t.data_ts) <= END_MS_OH]
    if len(option_emitted) < 5 or len(ohlcv_emitted) < 5:
        pytest.skip("Not enough ticks in window for parallel stability test")
    assert all(isinstance(t, IngestionTick) for t in option_emitted)
    assert all(isinstance(t, IngestionTick) for t in ohlcv_emitted)
    assert [t.data_ts for t in option_emitted] == sorted(t.data_ts for t in option_emitted)
    assert [t.data_ts for t in ohlcv_emitted] == sorted(t.data_ts for t in ohlcv_emitted)

    #### Test anti-lookahead for option chain handler ####
    
    chain_handler = OptionChainDataHandler("BTC")
    anchors = [
        option_emitted[0].data_ts,
        option_emitted[len(option_emitted) // 2].data_ts,
        option_emitted[-1].data_ts,
    ]
    for anchor_ts in anchors:
        chain_handler.align_to(anchor_ts)
        for tick in option_emitted:
            if tick.data_ts <= anchor_ts:
                chain_handler.on_new_tick(tick)
        snap = chain_handler.get_snapshot(anchor_ts)
        assert snap is None or int(snap.data_ts) <= anchor_ts


# --- NEW TEST: runtime grid probe anti-lookahead and no-drop parallel ---

@pytest.mark.local
@pytest.mark.asyncio
async def test_runtime_grid_probe_anti_lookahead_and_no_drop_parallel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run two ingestion workers + a third 'runtime grid' probe task concurrently.

    The probe continuously clamps handlers via align_to(anchor) and reads snapshots,
    asserting anti-lookahead during the live task loop (not after completion).

    Also asserts 'no-drop' semantics for each handler: when a new tick advances in
    event time, handler.last_timestamp() must advance to that tick's data_ts after ingest.

    Local-only test: requires existing cleaned datasets.
    """

    # --- local dataset existence guards ---
    base = Path("data/cleaned/option_chain/BTC/1m/2025")
    p1 = base / "2025_12_29.parquet"
    p2 = base / "2025_12_30.parquet"
    if not p1.exists() or not p2.exists():
        pytest.skip(f"Missing local data files: {p1} or {p2}")

    ohlcv_path = Path("data/cleaned/ohlcv/BTCUSDT/15m/2025.parquet")
    if not ohlcv_path.exists():
        pytest.skip(f"Missing local data file: {ohlcv_path}")

    # --- sources (production classes) ---
    option_source = OptionChainFileSource(
        root=Path("data/cleaned/option_chain"),
        asset="BTC",
        interval="1m",
        start_ts=START_MS,
        end_ts=END_MS,
    )
    ohlcv_source = OHLCVFileSource(
        root=Path("data/cleaned/ohlcv"),
        symbol="BTCUSDT",
        interval="15m",
        start_ts=START_MS_OH,
        end_ts=END_MS_OH,
    )

    # --- workers (production classes) ---
    option_worker = OptionChainWorker(
        source=option_source,
        normalizer=DeribitOptionChainNormalizer(symbol="BTC"),
        symbol="BTC",
        interval="1m",
        interval_ms=int(_to_interval_ms("1m") or 60_000),
        poll_interval_ms=0,
    )
    ohlcv_worker = OHLCVWorker(
        source=ohlcv_source,
        normalizer=BinanceOHLCVNormalizer(symbol="BTCUSDT"),
        symbol="BTCUSDT",
        interval="15m",
        interval_ms=int(_to_interval_ms("15m") or 900_000),
        poll_interval_ms=1,
    )

    # --- speed up: avoid real sleeps in worker loops ---
    orig_sleep = asyncio.sleep

    async def fast_sleep(_seconds: float) -> None:
        await orig_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    # --- runtime handlers under test (production classes) ---
    chain_handler = OptionChainDataHandler("BTC")
    ohlcv_handler = OHLCVDataHandler("BTCUSDT", interval="15m", cache={"maxlen": 10_000})

    # --- shared anchors updated by emit callbacks ---
    latest_chain_anchor: int | None = None
    latest_ohlcv_anchor: int | None = None

    # --- collected ticks for minimal sanity ---
    option_emitted: list[IngestionTick] = []
    ohlcv_emitted: list[IngestionTick] = []

    N = 200
    option_count = 0
    ohlcv_count = 0

    async def emit_option(tick: IngestionTick) -> None:
        nonlocal option_count, latest_chain_anchor
        option_emitted.append(tick)
        option_count += 1
        # clamp + ingest immediately, then assert anti-lookahead + no-drop
        anchor = int(tick.data_ts)
        latest_chain_anchor = anchor
        chain_handler.align_to(anchor)
        prev_last = chain_handler.last_timestamp()
        chain_handler.on_new_tick(tick)
        new_last = chain_handler.last_timestamp()
        assert new_last is None or new_last <= anchor
        if prev_last is not None and anchor > prev_last:
            assert new_last is not None
            assert new_last == anchor
        if option_count >= N:
            raise _StopReplay

    async def emit_ohlcv(tick: IngestionTick) -> None:
        nonlocal ohlcv_count, latest_ohlcv_anchor
        ohlcv_emitted.append(tick)
        ohlcv_count += 1
        anchor = int(tick.data_ts)
        latest_ohlcv_anchor = anchor
        ohlcv_handler.align_to(anchor)
        prev_last = ohlcv_handler.last_timestamp()
        ohlcv_handler.on_new_tick(tick)
        new_last = ohlcv_handler.last_timestamp()
        assert new_last is None or new_last <= anchor
        if prev_last is not None and anchor > prev_last:
            assert new_last is not None
            assert new_last == anchor
        if ohlcv_count >= N:
            raise _StopReplay

    async def _runtime_grid_probe() -> None:
        """Third task: continuously probe runtime reads while ingestion is running."""
        probes = 0
        # run until one of the workers stops the test
        while True:
            probes += 1

            # Option chain probe
            if latest_chain_anchor is not None:
                a = int(latest_chain_anchor)
                chain_handler.align_to(a)
                snap = chain_handler.get_snapshot(a)
                assert snap is None or int(snap.data_ts) <= a
                # window() returns snapshots; check max data_ts <= a
                w = chain_handler.window(a, n=5)
                if w:
                    assert max(int(s.data_ts) for s in w) <= a

            # OHLCV probe
            if latest_ohlcv_anchor is not None:
                a = int(latest_ohlcv_anchor)
                ohlcv_handler.align_to(a)
                snap = ohlcv_handler.get_snapshot(a)
                assert snap is None or int(snap.data_ts) <= a
                dfw = ohlcv_handler.window(a, n=5)
                if not dfw.empty and "data_ts" in dfw.columns:
                    assert int(dfw["data_ts"].max()) <= a

            # cooperative yield
            await orig_sleep(0)

    option_task = asyncio.create_task(option_worker.run(emit_option))
    ohlcv_task = asyncio.create_task(ohlcv_worker.run(emit_ohlcv))
    probe_task = asyncio.create_task(_runtime_grid_probe())

    try:
        done, pending = await asyncio.wait(
            {option_task, ohlcv_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            exc = task.exception()
            if isinstance(exc, _StopReplay):
                pass
            elif exc is not None:
                raise exc
    finally:
        # Cancel probe task and any pending worker tasks to prevent hangs
        probe_task.cancel()
        for task in (option_task, ohlcv_task):
            if not task.done():
                task.cancel()
        # Await all tasks with return_exceptions to ensure clean shutdown
        await asyncio.gather(probe_task, option_task, ohlcv_task, return_exceptions=True)

    # minimal sanity: we actually observed some ticks in-window
    option_emitted_w = [t for t in option_emitted if START_MS <= int(t.data_ts) <= END_MS]
    ohlcv_emitted_w = [t for t in ohlcv_emitted if START_MS_OH <= int(t.data_ts) <= END_MS_OH]
    assert len(option_emitted_w) > 0
    assert len(ohlcv_emitted_w) > 0
    assert [t.data_ts for t in option_emitted_w] == sorted(t.data_ts for t in option_emitted_w)
    assert [t.data_ts for t in ohlcv_emitted_w] == sorted(t.data_ts for t in ohlcv_emitted_w)


# --- NEW TEST: no-drop monotonic last_timestamp OHLCV bounded ---

@pytest.mark.local
@pytest.mark.asyncio
async def test_no_drop_monotonic_last_timestamp_ohlcv_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bounded OHLCV replay: when tick.data_ts advances, handler.last_timestamp must match it.

    This is a fast local sanity check for backtest-vs-realtime replay alignment.
    """

    path = Path("data/cleaned/ohlcv/BTCUSDT/15m/2025.parquet")
    if not path.exists():
        pytest.skip(f"Missing local data file: {path}")

    source = OHLCVFileSource(
        root=Path("data/cleaned/ohlcv"),
        symbol="BTCUSDT",
        interval="15m",
        start_ts=START_MS_OH,
        end_ts=END_MS_OH,
    )

    worker = OHLCVWorker(
        source=source,
        normalizer=BinanceOHLCVNormalizer(symbol="BTCUSDT"),
        symbol="BTCUSDT",
        interval="15m",
        interval_ms=int(_to_interval_ms("15m") or 900_000),
        poll_interval_ms=1,
    )

    orig_sleep = asyncio.sleep

    async def fast_sleep(_seconds: float) -> None:
        await orig_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    handler = OHLCVDataHandler("BTCUSDT", interval="15m", cache={"maxlen": 10_000})

    N = 200
    count = 0

    async def emit(tick: IngestionTick) -> None:
        nonlocal count
        count += 1
        anchor = int(tick.data_ts)
        handler.align_to(anchor)
        prev_last = handler.last_timestamp()
        handler.on_new_tick(tick)
        new_last = handler.last_timestamp()
        assert new_last is None or new_last <= anchor
        if prev_last is not None and anchor > prev_last:
            assert new_last is not None
            assert new_last == anchor
        if count >= N:
            raise _StopReplay

    task = asyncio.create_task(worker.run(emit))
    try:
        await task
    except _StopReplay:
        pass
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert count > 0
