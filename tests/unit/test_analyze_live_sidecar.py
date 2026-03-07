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
                "positions": {"BTCUSDT": {"qty": 0.01, "lots": 1, "entry_price": 100_000.0, "unrealized_pnl": 90.0}},
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
    trades = sidecar.api_payload("/api/trades", now_ms=1_700_000_001_000)
    signal = sidecar.api_payload("/api/signal", now_ms=1_700_000_001_000)
    raw_out = sidecar.api_payload("/api/raw-state", now_ms=1_700_000_001_000)

    assert status["schema"] == "system_status_v1"
    assert status["status"] == "healthy"
    assert status["data_feeds"][0]["domain"] == "ohlcv"
    assert status["data_feeds"][0]["symbol"] == "BTCUSDT"
    assert portfolio["schema"] == "portfolio_summary_v1"
    assert "snapshot_dict" not in str(portfolio)
    assert portfolio["positions"][0]["side"] == "LONG"
    assert perf["schema"] == "live_performance_v1"
    assert len(perf["equity_series"]) == 1
    assert trades["schema"] == "recent_trades_v1"
    assert trades["trades"][0]["quantity"] == 0.01
    assert signal["schema"] == "signal_snapshot_v1"
    assert "rsi" in signal["features"]
    assert raw_out == raw

