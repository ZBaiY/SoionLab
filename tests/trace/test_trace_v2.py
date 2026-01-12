from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest

from ingestion.contracts.tick import IngestionTick
from quant_engine.contracts.decision import DecisionBase
from quant_engine.contracts.model import ModelBase
from quant_engine.contracts.risk import RiskBase
from quant_engine.data.contracts.protocol_realtime import OHLCVHandlerProto
from quant_engine.portfolio.fractional import FractionalPortfolioManager
from quant_engine.portfolio.manager import PortfolioManager
from quant_engine.runtime.mock import MockDriver
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.utils.logger import (
    build_execution_constraints,
    build_trace_header,
    compute_config_hash,
    compute_intent_id,
    get_logger,
    init_logging,
    log_trace_header,
)


def _write_logging_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "logging.json"
    trace_path = tmp_path / "trace" / "{run_id}" / "trace.jsonl"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        json.dumps(
            {
                "active_profile": "default",
                "profiles": {
                    "default": {
                        "level": "INFO",
                        "format": {"json": True},
                        "handlers": {
                            "console": {"enabled": False},
                            "file": {"enabled": False},
                            "trace": {
                                "enabled": True,
                                "level": "INFO",
                                "path": str(trace_path),
                            },
                        },
                        "debug": {"enabled": False, "modules": []},
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    return config_path


def _reset_logging() -> None:
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()


def test_trace_header_first_and_minimal_fields(tmp_path: Path) -> None:
    config_path = _write_logging_config(tmp_path)
    init_logging(config_path=str(config_path), run_id="r1", mode="default")

    logger = get_logger("tests.trace")
    header = build_trace_header(
        run_id="r1",
        engine_mode="BACKTEST",
        config_hash="cfg",
        strategy_name="EXAMPLE",
        interval="1m",
        execution_constraints=build_execution_constraints(
            type("P", (), {"step_size": 1, "min_notional": 0.0})()
        ),
        start_ts_ms=123456,
        start_ts="2020-01-01T00:00:00+00:00",
    )
    log_trace_header(logger, header)

    from quant_engine.utils.logger import log_step_trace

    log_step_trace(
        logger,
        step_ts=123456,
        strategy="EXAMPLE",
        symbol="BTCUSDT",
        features={"f1": 1},
        models={"m1": {"score": 0.1}},
        portfolio={"cash": 100},
        primary_snapshots=None,
        market_snapshots=None,
        decision_score=0.5,
        target_position=1.0,
        fills=[],
        snapshot={"timestamp": 123456, "mode": "mock"},
    )

    out_path = tmp_path / "trace" / "r1" / "trace.jsonl"
    lines = out_path.read_text(encoding="utf-8").splitlines()
    assert lines
    header_payload = json.loads(lines[0])
    payload = json.loads(lines[1])

    assert header_payload["event"] == "trace.header"
    assert payload["event"] == "engine.step.trace"
    assert payload["run_id"] == "r1"
    assert payload["ts_ms"] == 123456
    for key in ("module", "msg", "category", "logger"):
        assert key not in payload

    _reset_logging()


def test_intent_id_is_stable_for_same_inputs() -> None:
    decision_inputs = {"features": {"x": 1}, "models": {"m": 0.2}}
    a = compute_intent_id(ts_ms=1, strategy="S", symbol="BTC", decision_inputs=decision_inputs)
    b = compute_intent_id(ts_ms=1, strategy="S", symbol="BTC", decision_inputs=decision_inputs)
    c = compute_intent_id(ts_ms=2, strategy="S", symbol="BTC", decision_inputs=decision_inputs)
    assert a == b
    assert a != c


class DummyFeatureExtractor:
    required_windows = {"ohlcv": 1}
    warmup_steps = 1

    def update(self, timestamp: int | None = None) -> dict:
        return {}

    def warmup(self, *, anchor_ts: int) -> None:
        return None

    def set_interval(self, interval: str | None) -> None:
        return None

    def set_interval_ms(self, interval_ms: int | None) -> None:
        return None


class DummyModel(ModelBase):
    def predict(self, features: dict) -> float:
        return 0.0


class ConstantDecision(DecisionBase):
    def decide(self, context: dict) -> float:
        return 0.5


class PassThroughRisk(RiskBase):
    def adjust(self, size: float, context: dict) -> float:
        return float(size)


class DummyExecutionEngine:
    def execute(
        self,
        timestamp: int,
        target_position: float,
        portfolio_state: dict,
        primary_snapshots,
    ) -> list[dict]:
        if target_position == 0:
            return []
        side = "BUY" if target_position > 0 else "SELL"
        qty = abs(float(target_position))
        filled_qty = qty if side == "BUY" else -qty
        return [
            {
                "symbol": "BTCUSDT",
                "timestamp": timestamp,
                "fill_price": 1.0,
                "filled_qty": filled_qty,
                "fee": 0.0,
                "side": side,
            }
        ]


class DummyOHLCVHandler:
    interval = "1m"
    interval_ms = 60_000

    def __init__(self) -> None:
        self._last_ts: int | None = None

    def load_history(self, *, start_ts: int | None = None, end_ts: int | None = None) -> None:
        return None

    def bootstrap(self, *, anchor_ts: int | None = None, lookback=None) -> None:
        return None

    def warmup_to(self, ts: int) -> None:
        self.align_to(ts)

    def align_to(self, ts: int) -> None:
        self._last_ts = int(ts)

    def last_timestamp(self) -> int | None:
        return self._last_ts

    def on_new_tick(self, tick: IngestionTick) -> None:
        self._last_ts = int(tick.data_ts)

    def get_snapshot(self, ts: int | None = None) -> dict:
        snap_ts = int(ts if ts is not None else (self._last_ts or 0))
        return {
            "data_ts": snap_ts,
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1.0,
        }

    def window(self, ts: int | None = None, n: int = 1) -> list[dict]:
        return [self.get_snapshot(ts)]


@dataclass(frozen=True)
class RunResult:
    header: dict
    steps: list[dict]


async def _run_once(tmp_path: Path, *, run_id: str, fractional: bool, config_hash: str) -> RunResult:
    config_path = _write_logging_config(tmp_path)
    init_logging(config_path=str(config_path), run_id=run_id, mode="default")

    handler = DummyOHLCVHandler()
    spec = EngineSpec.from_interval(mode=EngineMode.MOCK, interval="1m", symbol="BTCUSDT")
    execution_engine = DummyExecutionEngine()
    if fractional:
        portfolio = FractionalPortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_notional=0.0)
    else:
        portfolio = PortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_notional=0.0)

    engine = StrategyEngine(
        spec=spec,
        ohlcv_handlers={"BTCUSDT": cast(OHLCVHandlerProto, handler)},
        orderbook_handlers={},
        option_chain_handlers={},
        iv_surface_handlers={},
        sentiment_handlers={},
        trades_handlers={},
        option_trades_handlers={},
        feature_extractor=DummyFeatureExtractor(),
        models={"main": DummyModel(symbol="BTCUSDT")},
        decision=ConstantDecision(symbol="BTCUSDT"),
        risk_manager=PassThroughRisk(symbol="BTCUSDT"),
        execution_engine=execution_engine,
        portfolio_manager=portfolio,
        guardrails=True,
    )
    engine.strategy_name = "TRACE_TEST"
    engine.config_hash = config_hash

    logger = get_logger("tests.trace.run")
    log_trace_header(
        logger,
        build_trace_header(
            run_id=run_id,
            engine_mode=engine.spec.mode.value,
            config_hash=config_hash,
            strategy_name=engine.strategy_name,
            interval=engine.spec.interval,
            execution_constraints=build_execution_constraints(engine.portfolio),
            start_ts_ms=1_700_000_000_000,
            start_ts="2023-11-14T22:13:20+00:00",
        ),
    )

    driver = MockDriver(
        engine=engine,
        spec=engine.spec,
        timestamps=[1_700_000_000_000],
        ticks=[
            IngestionTick(
                timestamp=1_700_000_000_000,
                data_ts=1_700_000_000_000,
                domain="ohlcv",
                symbol="BTCUSDT",
                payload={"data_ts": 1_700_000_000_000},
            )
        ],
    )
    await driver.run()

    out_path = tmp_path / "trace" / run_id / "trace.jsonl"
    lines = out_path.read_text(encoding="utf-8").splitlines()
    header = json.loads(lines[0])
    steps = [json.loads(line) for line in lines[1:]]
    _reset_logging()
    return RunResult(header=header, steps=steps)


@pytest.mark.asyncio
async def test_trace_counterfactual_fractional(tmp_path: Path) -> None:
    base_cfg = {
        "strategy": "TRACE_TEST",
        "interval": "1m",
        "execution_constraints": {
            "fractional": False,
            "min_lot": 1.0,
            "min_notional": 0.0,
            "rounding_policy": "integer_floor",
        },
    }
    frac_cfg = dict(base_cfg)
    frac_cfg["execution_constraints"] = {
        "fractional": True,
        "min_lot": 0.0001,
        "min_notional": 0.0,
        "rounding_policy": "step_floor",
    }
    assert {k for k in base_cfg if base_cfg[k] != frac_cfg[k]} == {"execution_constraints"}

    base_hash = compute_config_hash(base_cfg)
    frac_hash = compute_config_hash(frac_cfg)
    assert base_hash != frac_hash

    base_run = await _run_once(tmp_path / "base", run_id="base", fractional=False, config_hash=base_hash)
    frac_run = await _run_once(tmp_path / "frac", run_id="frac", fractional=True, config_hash=frac_hash)

    base_intents = {(s["ts_ms"], s["intent_id"]) for s in base_run.steps}
    frac_intents = {(s["ts_ms"], s["intent_id"]) for s in frac_run.steps}
    assert base_intents == frac_intents

    base_outcome = base_run.steps[0]["execution_outcomes"][0]["execution_decision"]
    frac_outcome = frac_run.steps[0]["execution_outcomes"][0]["execution_decision"]
    assert base_outcome != frac_outcome
    assert base_run.header["config_hash"] != frac_run.header["config_hash"]
