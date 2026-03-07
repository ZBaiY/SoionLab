from __future__ import annotations

import json
from pathlib import Path

from analyze.backtest.reporter import generate_backtest_artifacts


def _write_trace(run_dir: Path) -> None:
    logs = run_dir / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "event": "trace.header",
            "schema_version": "trace_v2",
            "run_id": "r1",
            "engine_mode": "BACKTEST",
            "engine_git_sha": "abc",
            "config_hash": "cfg",
            "strategy_name": "EXAMPLE",
            "interval": "1m",
            "execution_constraints": {"fractional": True},
            "start_ts_ms": 1_000,
        },
        {
            "event": "engine.step.trace",
            "ts_ms": 1_000,
            "symbol": "BTCUSDT",
            "closed_bar_ready": True,
            "expected_visible_end_ts": 999,
            "actual_last_ts": 999,
            "portfolio": {"snapshot_dict": {"cash": 1000.0, "realized_pnl": 0.0, "unrealized_pnl": 0.0, "total_equity": 1000.0, "exposure": 0.0, "leverage": 0.0}},
            "market_snapshots": {"ohlcv": {"BTCUSDT": {"numeric": {"data_ts": 999}}}},
            "fills": [{"symbol": "BTCUSDT", "side": "BUY", "filled_qty": 1.0, "fill_price": 100.0, "fee": 0.1, "execution_decision": "ACCEPTED"}],
        },
        {
            "event": "engine.step.trace",
            "ts_ms": 2_000,
            "symbol": "BTCUSDT",
            "closed_bar_ready": True,
            "expected_visible_end_ts": 1999,
            "actual_last_ts": 1999,
            "portfolio": {"snapshot_dict": {"cash": 900.0, "realized_pnl": 0.0, "unrealized_pnl": 10.0, "total_equity": 1010.0, "exposure": 110.0, "leverage": 0.1}},
            "market_snapshots": {"ohlcv": {"BTCUSDT": {"numeric": {"data_ts": 1999}}}},
            "fills": [],
        },
        {
            "event": "engine.step.trace",
            "ts_ms": 3_000,
            "symbol": "BTCUSDT",
            "closed_bar_ready": True,
            "expected_visible_end_ts": 2999,
            "actual_last_ts": 2999,
            "portfolio": {"snapshot_dict": {"cash": 1010.0, "realized_pnl": 10.0, "unrealized_pnl": 0.0, "total_equity": 1010.0, "exposure": 0.0, "leverage": 0.0}},
            "market_snapshots": {"ohlcv": {"BTCUSDT": {"numeric": {"data_ts": 2999}}}},
            "fills": [{"symbol": "BTCUSDT", "side": "SELL", "filled_qty": -1.0, "fill_price": 110.0, "fee": 0.1, "execution_decision": "ACCEPTED"}],
        },
    ]
    with (logs / "trace.jsonl").open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def test_generate_backtest_artifacts_writes_report_and_views(tmp_path: Path) -> None:
    run_dir = tmp_path / "artifacts" / "runs" / "r1"
    _write_trace(run_dir)

    out = generate_backtest_artifacts(run_dir=run_dir, run_id="r1")
    assert "report_path" in out
    assert (run_dir / "report" / "report.json").exists()
    assert (run_dir / "report" / "summary.json").exists()
    assert (run_dir / "views" / "backtest_card.json").exists()
    assert (run_dir / "views" / "equity_chart.json").exists()
    assert (run_dir / "views" / "drawdown_chart.json").exists()
    assert (run_dir / "views" / "performance_summary.json").exists()
    assert (run_dir / "views" / "warning_list.json").exists()
    assert (run_dir / "views" / "pnl_period_chart.json").exists()
    assert (run_dir / "views" / "exposure_chart.json").exists()
    assert (run_dir / "views" / "trade_pnl_histogram.json").exists()
    assert (run_dir / "views" / "trade_list.json").exists()
    assert (run_dir / "views" / "quality_badge.json").exists()
    assert (run_dir.parent / "_index.json").exists()

    report = json.loads((run_dir / "report" / "report.json").read_text(encoding="utf-8"))
    card = json.loads((run_dir / "views" / "backtest_card.json").read_text(encoding="utf-8"))
    chart = json.loads((run_dir / "views" / "equity_chart.json").read_text(encoding="utf-8"))
    drawdown = json.loads((run_dir / "views" / "drawdown_chart.json").read_text(encoding="utf-8"))
    perf_summary = json.loads((run_dir / "views" / "performance_summary.json").read_text(encoding="utf-8"))
    warnings = json.loads((run_dir / "views" / "warning_list.json").read_text(encoding="utf-8"))
    pnl_period = json.loads((run_dir / "views" / "pnl_period_chart.json").read_text(encoding="utf-8"))
    exposure = json.loads((run_dir / "views" / "exposure_chart.json").read_text(encoding="utf-8"))
    trade_hist = json.loads((run_dir / "views" / "trade_pnl_histogram.json").read_text(encoding="utf-8"))
    trades = json.loads((run_dir / "views" / "trade_list.json").read_text(encoding="utf-8"))
    quality = json.loads((run_dir / "views" / "quality_badge.json").read_text(encoding="utf-8"))
    run_index = json.loads((run_dir.parent / "_index.json").read_text(encoding="utf-8"))

    assert report["report_schema_version"] == "report_v1"
    assert len(report["exposure_curve"]) == 3
    assert card["schema"] == "backtest_card_v1"
    assert card["performance"]["total_return_pct"] == 1.0
    assert card["time_range"]["start_ms"] == 1000
    assert isinstance(card["time_range"]["start"], str)
    assert chart["schema"] == "equity_chart_v1"
    assert len(chart["series"]) == 3
    assert chart["series"][0]["ts_ms"] == 1000
    assert isinstance(chart["series"][0]["ts"], str)
    assert chart["series"][0]["total_equity"] == 1000.0
    assert "t" not in chart["series"][0]
    assert "equity" not in chart["series"][0]
    assert drawdown["schema"] == "drawdown_chart_v1"
    assert drawdown["series"][0]["ts_ms"] == 1000
    assert isinstance(drawdown["series"][0]["ts"], str)
    assert perf_summary["schema"] == "performance_summary_v1"
    assert perf_summary["metrics"]["total_return_pct"] == 1.0
    assert "win_rate_pct" in perf_summary["metrics"]
    assert warnings["schema"] == "warning_list_v1"
    assert isinstance(warnings["items"], list)
    assert pnl_period["schema"] == "pnl_period_chart_v1"
    assert pnl_period["series"][0]["start_ts_ms"] == 1000
    assert isinstance(pnl_period["series"][0]["start_ts"], str)
    assert exposure["schema"] == "exposure_chart_v1"
    assert exposure["series"][1]["exposure"] == 110.0
    assert trade_hist["schema"] == "trade_pnl_histogram_v1"
    assert isinstance(trade_hist["buckets"], list)
    assert trades["schema"] == "trade_list_v1"
    assert trades["pagination"]["total"] == 1
    assert trades["trades"][0]["quantity"] == 1.0
    assert quality["schema"] == "quality_badge_v1"
    assert quality["verdict"] == report["quality"]["verdict"]
    assert run_index["schema"] == "run_index_v1"
    assert run_index["runs"][0]["run_id"] == "r1"
