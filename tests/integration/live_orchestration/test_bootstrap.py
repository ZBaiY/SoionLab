from __future__ import annotations

from collections import defaultdict
import logging

import pytest

from quant_engine.runtime.modes import EngineMode


def test_bootstrap_expands_window_by_warmup_steps(make_engine) -> None:
    engine = make_engine(mode=EngineMode.MOCK, required_windows={"ohlcv": 5}, warmup_steps=3, include_option_chain=False)
    handler = engine.ohlcv_handlers["BTCUSDT"]
    calls: list[tuple[int | None, int | None]] = []

    def _spy_bootstrap(*, anchor_ts=None, lookback=None):
        calls.append((anchor_ts, lookback))

    handler.bootstrap = _spy_bootstrap  # type: ignore[method-assign]
    anchor = 1_704_067_320_000
    engine.preload_data(anchor_ts=anchor)

    assert len(calls) == 1
    assert calls[0][0] == anchor
    assert calls[0][1] == 8


def test_bootstrap_logs_partial_fill(caplog: pytest.LogCaptureFixture, make_engine) -> None:
    engine = make_engine(mode=EngineMode.MOCK, required_windows={"ohlcv": 5}, warmup_steps=2, include_option_chain=False)
    handler = engine.ohlcv_handlers["BTCUSDT"]

    def _noop_bootstrap(*, anchor_ts=None, lookback=None):
        return None

    handler.bootstrap = _noop_bootstrap  # type: ignore[method-assign]

    with caplog.at_level(logging.INFO):
        engine.preload_data(anchor_ts=1_900_000_000_000)

    records = [rec for rec in caplog.records if "engine.preload.partial_fill" in rec.getMessage()]
    assert records
    assert records[0].levelno == logging.INFO


def test_bootstrap_backfill_cooldown_across_handlers(make_engine) -> None:
    engine = make_engine(mode=EngineMode.MOCK, required_windows={"ohlcv": 1, "option_chain": 1}, warmup_steps=1)
    counts: dict[str, int] = defaultdict(int)

    for sym, h in engine.ohlcv_handlers.items():
        h._maybe_backfill = (lambda *, target_ts, _k=f"ohlcv:{sym}": counts.__setitem__(_k, counts[_k] + 1))  # type: ignore[attr-defined]
    for sym, h in engine.option_chain_handlers.items():
        h._maybe_backfill = (lambda *, target_ts, _k=f"option_chain:{sym}": counts.__setitem__(_k, counts[_k] + 1))  # type: ignore[attr-defined]

    ts = 1_704_067_320_000
    engine.align_to(ts)
    engine.align_to(ts)
    engine.align_to(ts + 60_000)

    assert counts["ohlcv:BTCUSDT"] == 2
    assert counts["option_chain:BTCUSDT"] == 2
    assert engine.option_chain_handlers["BTCUSDT"]._should_backfill() is False
