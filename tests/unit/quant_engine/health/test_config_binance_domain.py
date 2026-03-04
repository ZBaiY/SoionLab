from quant_engine.health.config import backtest_config, default_realtime_config


def test_default_realtime_includes_execution_binance_domain() -> None:
    cfg = default_realtime_config(interval_ms=60_000)
    assert "execution/binance" in cfg.domains
    dom = cfg.domains["execution/binance"]
    assert dom.criticality == "hard"
    assert dom.max_restarts == 0


def test_backtest_includes_execution_binance_domain() -> None:
    cfg = backtest_config(interval_ms=60_000)
    assert "execution/binance" in cfg.domains
