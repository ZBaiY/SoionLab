from __future__ import annotations

from analyze.live.sidecar import LiveSidecarState


def test_live_sidecar_exposes_portal_contracts_and_raw_escape_hatch() -> None:
    sidecar = LiveSidecarState(
        run_id="live1",
        strategy="EXAMPLE",
        symbol="BTCUSDT",
        interval="1m",
        equity_window_steps=3,
        trades_window=2,
    )
    raw = {
        "timestamp": 1_700_000_000_000,
        "features": {"rsi": 50.0},
        "model_outputs": {"main": {"score": 0.2}},
        "decision_score": 0.2,
        "target_position": 0.1,
        "fills": [{"symbol": "BTCUSDT", "side": "BUY", "filled_qty": 0.01, "fill_price": 100_000.0, "fee": 0.1}],
        "portfolio": {
            "snapshot_dict": {
                "cash": 900.0,
                "total_equity": 1000.0,
                "realized_pnl": 10.0,
                "unrealized_pnl": 90.0,
                "exposure": 100.0,
                "leverage": 0.1,
                "positions": {
                    "BTCUSDT": {"qty": 0.01, "lots": 1, "entry_price": 100_000.0, "unrealized_pnl": 90.0},
                    "ETHUSDT": {"qty": 0.0, "lots": 2, "entry_price": 5_000.0, "unrealized_pnl": 0.0},
                },
            }
        },
        "health": {
            "global_mode": "running",
            "execution_permit": "full",
            "consecutive_skips": 0,
            "consecutive_step_failures": 0,
            "last_step_ts": 1_700_000_000_000,
            "domains": {"ohlcv:BTCUSDT:source": {"state": "healthy", "staleness_ms": 100, "fault_count": 0}},
        },
    }
    sidecar.ingest_raw(raw, now_ms=1_700_000_000_500)

    status = sidecar.api_payload("/api/status", now_ms=1_700_000_001_000)
    portfolio = sidecar.api_payload("/api/portfolio", now_ms=1_700_000_001_000)
    perf = sidecar.api_payload("/api/performance", now_ms=1_700_000_001_000)
    freshness = sidecar.api_payload("/api/freshness", now_ms=1_700_000_001_000)
    trades = sidecar.api_payload("/api/trades", now_ms=1_700_000_001_000)
    signal = sidecar.api_payload("/api/signal", now_ms=1_700_000_001_000)
    raw_out = sidecar.api_payload("/api/raw-state", now_ms=1_700_000_001_000)

    assert status["schema"] == "system_status_v1"
    assert status["as_of"] == "2023-11-14T22:13:20Z"
    assert status["as_of_ms"] == 1_700_000_000_000
    assert status["status"] == "healthy"
    assert status["status_label"] == "Healthy"
    assert status["execution_permit"] == "full"
    assert status["data_feeds"][0]["domain"] == "ohlcv"
    assert status["data_feeds"][0]["symbol"] == "BTCUSDT"
    assert portfolio["schema"] == "portfolio_summary_v1"
    assert "snapshot_dict" not in str(portfolio)
    assert portfolio["as_of"] == "2023-11-14T22:13:20Z"
    assert portfolio["as_of_ms"] == 1_700_000_000_000
    assert portfolio["gross_exposure"] == 100.0
    assert portfolio["leverage_pct"] == 10.0
    assert len(portfolio["positions"]) == 1
    assert portfolio["positions"][0]["side"] == "LONG"
    assert portfolio["positions"][0]["symbol"] == "BTCUSDT"
    assert perf["schema"] == "live_performance_v1"
    assert perf["as_of"] == "2023-11-14T22:13:20Z"
    assert perf["as_of_ms"] == 1_700_000_000_000
    assert perf["session_start"] is not None
    assert perf["session_start_ms"] > 0
    assert perf["current_equity"] == 1000.0
    assert len(perf["equity_series"]) == 1
    assert perf["equity_series"][0] == {
        "ts": "2023-11-14T22:13:20Z",
        "ts_ms": 1_700_000_000_000,
        "equity": 1000.0,
    }
    assert "t" not in perf["equity_series"][0]
    assert "v" not in perf["equity_series"][0]
    assert freshness["schema"] == "freshness_metadata_v1"
    assert freshness["as_of"] == "2023-11-14T22:13:20Z"
    assert freshness["as_of_ms"] == 1_700_000_000_000
    assert freshness["freshness_ms"] == 1_000
    assert freshness["snapshot_age_label"] == "recent"
    assert freshness["is_stale"] is False
    assert freshness["last_step_ts"] == "2023-11-14T22:13:20Z"
    assert freshness["last_step_ts_ms"] == 1_700_000_000_000
    assert freshness["last_step_age_ms"] == 1_000
    assert freshness["feeds"] == [
        {
            "domain": "ohlcv",
            "symbol": "BTCUSDT",
            "staleness_ms": 100,
            "state": "healthy",
        }
    ]
    assert trades["schema"] == "recent_trades_v1"
    assert trades["as_of"] == "2023-11-14T22:13:20Z"
    assert trades["as_of_ms"] == 1_700_000_000_000
    assert trades["trades"][0]["quantity"] == 0.01
    assert trades["trades"][0]["side"] == "BUY"
    assert trades["trades"][0]["source"] == "simulated"
    assert signal["schema"] == "signal_snapshot_v1"
    assert "rsi" in signal["features"]
    assert raw_out == raw


def test_live_sidecar_freshness_contract_signals_stale_state() -> None:
    sidecar = LiveSidecarState(
        run_id="live2",
        strategy="EXAMPLE",
        symbol="BTCUSDT",
        interval="1m",
    )
    raw = {
        "timestamp": 1_700_000_000_000,
        "portfolio": {"snapshot_dict": {"cash": 1000.0, "total_equity": 1000.0, "positions": {}}},
        "health": {
            "global_mode": "degraded",
            "execution_permit": "reduce_only",
            "last_step_ts": 1_700_000_000_000,
            "domains": {"ohlcv:BTCUSDT:source": {"state": "stale", "staleness_ms": 45_000, "fault_count": 2}},
        },
    }
    sidecar.ingest_raw(raw, now_ms=1_700_000_000_000)

    freshness = sidecar.api_payload("/api/freshness", now_ms=1_700_000_045_000)

    assert freshness["schema"] == "freshness_metadata_v1"
    assert freshness["snapshot_age_label"] == "stale"
    assert freshness["is_stale"] is True
    assert freshness["last_step_age_ms"] == 45_000
    assert freshness["feeds"][0]["domain"] == "ohlcv"
    assert freshness["feeds"][0]["state"] == "stale"


def test_live_sidecar_recent_trades_contract_is_well_formed_when_empty() -> None:
    sidecar = LiveSidecarState(
        run_id="live3",
        strategy="EXAMPLE",
        symbol="BTCUSDT",
        interval="1m",
    )
    raw = {
        "timestamp": 1_700_000_100_000,
        "portfolio": {"snapshot_dict": {"cash": 1000.0, "total_equity": 1000.0, "positions": {}}},
        "health": {
            "global_mode": "running",
            "execution_permit": "full",
            "last_step_ts": 1_700_000_100_000,
            "domains": {},
        },
    }
    sidecar.ingest_raw(raw, now_ms=1_700_000_100_000)

    trades = sidecar.api_payload("/api/trades", now_ms=1_700_000_101_000)

    assert trades["schema"] == "recent_trades_v1"
    assert trades["as_of"] == "2023-11-14T22:15:00Z"
    assert trades["as_of_ms"] == 1_700_000_100_000
    assert trades["trades"] == []
    assert trades["window_steps"] == 1
    assert trades["total_fills_session"] == 0
