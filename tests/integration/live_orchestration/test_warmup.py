from __future__ import annotations

import logging

import pandas as pd
import pytest

from quant_engine.runtime.modes import EngineMode


def test_warmup_hard_domain_raises_after_failed_backfill(make_engine) -> None:
    engine = make_engine(mode=EngineMode.MOCK, required_windows={"ohlcv": 3}, warmup_steps=1, include_option_chain=False)
    h = engine.ohlcv_handlers["BTCUSDT"]

    h.bootstrap = (lambda *, anchor_ts=None, lookback=None: None)  # type: ignore[method-assign]
    h.window = (lambda ts, n: pd.DataFrame(columns=["data_ts"]))  # type: ignore[method-assign]
    h._backfill_from_source = (lambda *, start_ts, end_ts, target_ts: 0)  # type: ignore[attr-defined]

    anchor = 1_704_067_320_000
    engine.preload_data(anchor_ts=anchor)
    with pytest.raises(RuntimeError, match="insufficient history after backfill"):
        engine.warmup_features(anchor_ts=anchor)


def test_warmup_soft_domain_warns_and_skips(caplog: pytest.LogCaptureFixture, make_engine, fresh_ohlcv_window) -> None:
    engine = make_engine(mode=EngineMode.MOCK, required_windows={"ohlcv": 2, "option_chain": 5}, warmup_steps=1)
    ohlcv = engine.ohlcv_handlers["BTCUSDT"]
    option_chain = engine.option_chain_handlers["BTCUSDT"]

    def _ohlcv_window(ts, n):
        return fresh_ohlcv_window(int(ts), int(n), int(ohlcv.interval_ms))

    ohlcv.window = _ohlcv_window  # type: ignore[method-assign]
    option_chain.window = (lambda ts, n: [])  # type: ignore[method-assign]
    called = {"n": 0}

    def _option_backfill(*, start_ts, end_ts, target_ts):
        called["n"] += 1
        return 0

    option_chain._backfill_from_source = _option_backfill  # type: ignore[attr-defined]

    anchor = 1_704_067_320_000
    engine.preload_data(anchor_ts=anchor)
    with caplog.at_level(logging.WARNING):
        engine.warmup_features(anchor_ts=anchor)

    assert called["n"] == 0
    assert any("engine.warmup.backfill.soft_domain_skipped" in rec.getMessage() for rec in caplog.records)
    assert any("engine.warmup.soft_domain_insufficient" in rec.getMessage() for rec in caplog.records)


def test_warmup_continuity_spotcheck(make_engine) -> None:
    engine = make_engine(mode=EngineMode.MOCK, required_windows={"ohlcv": 10}, warmup_steps=1, include_option_chain=False)
    h = engine.ohlcv_handlers["BTCUSDT"]
    state = {"gappy": True, "backfill_calls": 0}

    def _window(ts, n):
        n = int(n)
        anchor = int(ts)
        rows: list[dict[str, int | float]] = []
        for i in range(n):
            t = anchor - (n - i - 1) * int(h.interval_ms)
            rows.append({"data_ts": t, "close": 1.0})
        df = pd.DataFrame(rows)
        if state["gappy"] and len(df) >= 7:
            df.loc[6, "data_ts"] = int(df.loc[5, "data_ts"]) + 5 * int(h.interval_ms)
        return df

    def _backfill(*, start_ts, end_ts, target_ts):
        state["backfill_calls"] += 1
        state["gappy"] = False
        return 1

    h.window = _window  # type: ignore[method-assign]
    h._backfill_from_source = _backfill  # type: ignore[attr-defined]

    anchor = 1_704_067_320_000
    engine.preload_data(anchor_ts=anchor)
    engine.warmup_features(anchor_ts=anchor)

    assert state["backfill_calls"] >= 1
