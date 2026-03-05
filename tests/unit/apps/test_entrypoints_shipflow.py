from __future__ import annotations

import asyncio
import time
import signal
from pathlib import Path
from types import SimpleNamespace

import pytest

import apps.run_backtest as run_backtest
import apps.run_code.backtest_app as backtest_app
import apps.run_code.realtime_app as realtime_app
import apps.run_mock as run_mock
import apps.run_realtime as run_realtime
import apps.run_sample as run_sample
from quant_engine.execution.exchange.binance_client import BinanceClientConfig


def test_set_current_run_symlink_points_to_run_id_backtest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    backtest_app._set_current_run("r1")
    current = tmp_path / "artifacts" / "runs" / "_current"
    assert current.is_symlink()
    assert current.readlink() == Path("r1")
    assert current.resolve() == (tmp_path / "artifacts" / "runs" / "r1").resolve()


def test_set_current_run_symlink_points_to_run_id_realtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    run_realtime._set_current_run("r2")
    current = tmp_path / "artifacts" / "runs" / "_current"
    assert current.is_symlink()
    assert current.readlink() == Path("r2")
    assert current.resolve() == (tmp_path / "artifacts" / "runs" / "r2").resolve()


def test_set_current_run_fallback_current_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    def _raise_symlink(self, target, target_is_directory=False):  # noqa: ANN001
        raise NotImplementedError("no symlink")

    monkeypatch.setattr(Path, "symlink_to", _raise_symlink)
    backtest_app._set_current_run("r3")
    assert (tmp_path / "artifacts" / "runs" / "CURRENT").read_text(encoding="utf-8") == "r3"


def test_run_backtest_cli_plumbs_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_backtest_app(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(run_backtest, "run_backtest_app", _fake_run_backtest_app)
    asyncio.run(
        run_backtest.main(
            [
                "--strategy",
                "EXAMPLE",
                "--symbols",
                "A=BTCUSDT,B=ETHUSDT",
                "--start-ts",
                "1000",
                "--end-ts",
                "2000",
                "--data-root",
                "./data",
                "--run-id",
                "rid-1",
            ]
        )
    )

    assert captured["strategy_name"] == "EXAMPLE"
    assert captured["bind_symbols"] == {"A": "BTCUSDT", "B": "ETHUSDT"}
    assert captured["start_ts"] == 1000
    assert captured["end_ts"] == 2000
    assert captured["data_root"] == Path("./data")
    assert captured["run_id"] == "rid-1"


def test_run_mock_cli_plumbs_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeDriver:
        def __init__(self, **kwargs):
            self._kwargs = kwargs

        async def run(self) -> None:
            return None

    fake_engine = SimpleNamespace(
        spec=SimpleNamespace(mode=SimpleNamespace(value="MOCK"), interval="1m"),
        portfolio=SimpleNamespace(step_size=1.0, min_notional=10.0),
        config_hash="h",
        strategy_name="EXAMPLE",
    )

    captured: dict[str, object] = {}

    def _fake_build_mock_engine(**kwargs):
        captured.update(kwargs)
        return fake_engine, {"timestamps": [1, 2], "ticks": []}, []

    monkeypatch.setattr(run_mock, "build_mock_engine", _fake_build_mock_engine)
    monkeypatch.setattr(run_mock, "MockDriver", _FakeDriver)
    monkeypatch.setattr(run_mock, "init_logging", lambda run_id: None)
    monkeypatch.setattr(run_mock, "_set_current_run", lambda run_id: None)

    asyncio.run(
        run_mock.main(
            [
                "--strategy",
                "EXAMPLE",
                "--symbols",
                "A=BTCUSDT,B=ETHUSDT",
                "--timestamps",
                "111,222",
                "--run-id",
                "mock-rid",
            ]
        )
    )

    assert captured["strategy_name"] == "EXAMPLE"
    assert captured["bind_symbols"] == {"A": "BTCUSDT", "B": "ETHUSDT"}
    assert captured["timestamps"] == [111, 222]


def test_run_sample_parser_accepts_shipflow_args() -> None:
    parser = run_sample._build_parser()
    ns = parser.parse_args(
        [
            "--strategy",
            "EXAMPLE",
            "--symbols",
            "A=BTCUSDT,B=ETHUSDT",
            "--start-ts",
            "1",
            "--end-ts",
            "2",
            "--data-root",
            "./data",
            "--run-id",
            "rid-sample",
        ]
    )
    assert ns.strategy == "EXAMPLE"
    assert ns.symbols == "A=BTCUSDT,B=ETHUSDT"
    assert ns.start_ts == 1
    assert ns.end_ts == 2


def test_realtime_preflight_fails_fast_on_live_binance(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(realtime_app, "_matching_type_for_strategy", lambda **_: "LIVE-BINANCE")

    def _raise(*args, **kwargs):  # noqa: ANN002, ANN003
        raise run_realtime.BinanceClientError("missing key")

    monkeypatch.setattr(realtime_app, "resolve_binance_profile", _raise)
    with pytest.raises(RuntimeError, match="Realtime preflight failed"):
        realtime_app._validate_realtime_preflight(
            strategy_name="EXAMPLE",
            bind_symbols={"A": "BTCUSDT"},
            binance_env="testnet",
            binance_base_url=None,
        )


def test_realtime_preflight_mainnet_requires_confirm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(realtime_app, "_matching_type_for_strategy", lambda **_: "LIVE-BINANCE")
    cfg = BinanceClientConfig(
        api_key="k",
        api_secret="s",
        base_url="https://api.binance.com",
        env="mainnet",
    )
    monkeypatch.setattr(realtime_app, "resolve_binance_profile", lambda **kwargs: cfg)
    monkeypatch.delenv("BINANCE_MAINNET_CONFIRM", raising=False)
    with pytest.raises(RuntimeError, match="BINANCE_MAINNET_CONFIRM=YES"):
        realtime_app._validate_realtime_preflight(
            strategy_name="EXAMPLE",
            bind_symbols={"A": "BTCUSDT"},
            binance_env="mainnet",
            binance_base_url=None,
        )


def test_realtime_preflight_allows_privileged_base_url_override_with_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(realtime_app, "_matching_type_for_strategy", lambda **_: "LIVE-BINANCE")
    monkeypatch.setenv("BINANCE_BASE_URL_CONFIRM", "YES")
    monkeypatch.setenv("BINANCE_MAINNET_CONFIRM", "YES")
    cfg = BinanceClientConfig(
        api_key="k",
        api_secret="s",
        base_url="https://api.binance.com",
        env="mainnet",
    )
    monkeypatch.setattr(realtime_app, "resolve_binance_profile", lambda **kwargs: cfg)
    realtime_app._validate_realtime_preflight(
        strategy_name="EXAMPLE",
        bind_symbols={"A": "BTCUSDT"},
        binance_env="mainnet",
        binance_base_url="https://api.binance.com",
    )


def test_realtime_main_sigint_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeDriver:
        def __init__(self, *, engine, spec, stop_event):
            self._stop_event = stop_event

        async def run(self) -> None:
            while not self._stop_event.is_set():
                await asyncio.sleep(0.01)

    fake_engine = SimpleNamespace(
        spec=SimpleNamespace(mode=SimpleNamespace(value="REALTIME"), interval="1m"),
        portfolio=SimpleNamespace(step_size=1.0, min_notional=10.0),
        config_hash="h",
        strategy_name="EXAMPLE",
        _health=None,
    )

    monkeypatch.setattr(run_realtime, "init_logging", lambda run_id: None)
    monkeypatch.setattr(run_realtime, "_set_current_run", lambda run_id: None)
    monkeypatch.setattr(run_realtime, "_validate_realtime_preflight", lambda **kwargs: None)
    monkeypatch.setattr(
        run_realtime,
        "build_realtime_engine",
        lambda **kwargs: (fake_engine, {}, []),
    )
    monkeypatch.setattr(run_realtime, "RealtimeDriver", _FakeDriver)

    def _fake_install(loop, on_signal):
        loop.call_soon(on_signal, signal.SIGINT)
        return "loop", [], {}

    monkeypatch.setattr(run_realtime, "_install_signal_handlers", _fake_install)

    asyncio.run(run_realtime.main(["--strategy", "EXAMPLE"]))


def test_realtime_main_sigint_shutdown_fallback_without_add_signal_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeDriver:
        def __init__(self, *, engine, spec, stop_event):
            self._stop_event = stop_event

        async def run(self) -> None:
            while not self._stop_event.is_set():
                await asyncio.sleep(0.01)

    fake_engine = SimpleNamespace(
        spec=SimpleNamespace(mode=SimpleNamespace(value="REALTIME"), interval="1m"),
        portfolio=SimpleNamespace(step_size=1.0, min_notional=10.0),
        config_hash="h",
        strategy_name="EXAMPLE",
        _health=None,
    )
    installed_handlers: dict[signal.Signals, object] = {}

    def _fake_signal(sig, handler):  # noqa: ANN001
        installed_handlers[sig] = handler
        return signal.SIG_DFL

    monkeypatch.setattr(run_realtime, "init_logging", lambda run_id: None)
    monkeypatch.setattr(run_realtime, "_set_current_run", lambda run_id: None)
    monkeypatch.setattr(run_realtime, "_validate_realtime_preflight", lambda **kwargs: None)
    monkeypatch.setattr(run_realtime, "build_realtime_engine", lambda **kwargs: (fake_engine, {}, []))
    monkeypatch.setattr(run_realtime, "RealtimeDriver", _FakeDriver)
    monkeypatch.setattr(run_realtime.signal, "signal", _fake_signal)
    monkeypatch.setattr(run_realtime.signal, "getsignal", lambda sig: signal.SIG_DFL)

    real_get_running_loop = run_realtime.asyncio.get_running_loop

    def _fake_get_running_loop():
        real_loop = real_get_running_loop()

        class _LoopProxy:
            def add_signal_handler(self, *args, **kwargs):  # noqa: ANN002, ANN003
                raise NotImplementedError("forced fallback path")

            def call_soon_threadsafe(self, callback, *args):  # noqa: ANN001
                return real_loop.call_soon_threadsafe(callback, *args)

            def remove_signal_handler(self, sig):  # noqa: ANN001
                return real_loop.remove_signal_handler(sig)

        return _LoopProxy()

    monkeypatch.setattr(run_realtime.asyncio, "get_running_loop", _fake_get_running_loop)

    async def _trigger_sigint_soon() -> None:
        while signal.SIGINT not in installed_handlers:
            await asyncio.sleep(0.01)
        handler = installed_handlers[signal.SIGINT]
        handler(signal.SIGINT.value, None)

    real_main = run_realtime.main

    async def _wrapped_main(argv):  # noqa: ANN001
        trigger = asyncio.create_task(_trigger_sigint_soon())
        try:
            await real_main(argv)
        finally:
            trigger.cancel()
            await asyncio.gather(trigger, return_exceptions=True)

    started = time.monotonic()
    asyncio.run(_wrapped_main(["--strategy", "EXAMPLE"]))
    assert time.monotonic() - started < 2.0


def test_build_realtime_engine_forwards_stop_event_to_ingestion_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeStrategy:
        REQUIRED_DATA = set()

        @staticmethod
        def standardize(*, overrides, symbols):  # noqa: ANN001
            return SimpleNamespace(interval="1m", execution={})

    fake_engine = SimpleNamespace(
        ohlcv_handlers={},
        orderbook_handlers={},
        option_chain_handlers={},
        iv_surface_handlers={},
        sentiment_handlers={},
        trades_handlers={},
        option_trades_handlers={},
    )
    seen: dict[str, object] = {}

    def _fake_ingestion_plan(engine, *, required_domains, stop_event=None, deribit_base_url=None):  # noqa: ANN001
        seen["stop_event"] = stop_event
        seen["deribit_base_url"] = deribit_base_url
        return []

    monkeypatch.setattr(realtime_app, "get_strategy", lambda name: _FakeStrategy)
    monkeypatch.setattr(realtime_app.StrategyLoader, "from_config", lambda **kwargs: fake_engine)
    monkeypatch.setattr(realtime_app, "_build_realtime_ingestion_plan", _fake_ingestion_plan)

    stop_event = SimpleNamespace(is_set=lambda: False)
    realtime_app.build_realtime_engine(
        strategy_name="EXAMPLE",
        bind_symbols={"A": "BTCUSDT"},
        stop_event=stop_event,  # type: ignore[arg-type]
        deribit_base_url="https://test.deribit.com",
    )

    assert seen["stop_event"] is stop_event
    assert seen["deribit_base_url"] == "https://test.deribit.com"


def test_realtime_preflight_blocks_privileged_base_url_override_and_skips_engine_build(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    build_calls = {"count": 0}
    monkeypatch.setattr(realtime_app, "_matching_type_for_strategy", lambda **_: "LIVE-BINANCE")
    monkeypatch.setattr(run_realtime, "init_logging", lambda run_id: None)
    monkeypatch.setattr(run_realtime, "_set_current_run", lambda run_id: None)
    monkeypatch.setattr(
        run_realtime,
        "build_realtime_engine",
        lambda **kwargs: (build_calls.__setitem__("count", build_calls["count"] + 1), {}, []),
    )
    monkeypatch.delenv("BINANCE_BASE_URL_CONFIRM", raising=False)
    monkeypatch.delenv("BINANCE_PROXY_MODE", raising=False)

    with caplog.at_level("WARNING"):
        with pytest.raises(RuntimeError, match="BINANCE_BASE_URL override is privileged"):
            asyncio.run(
                run_realtime.main(
                    [
                        "--strategy",
                        "EXAMPLE",
                        "--binance-env",
                        "mainnet",
                        "--binance-base-url",
                        "https://api.binance.com",
                    ]
                )
            )

    assert build_calls["count"] == 0
    assert any(rec.message == "binance.base_url.override.blocked" for rec in caplog.records)
