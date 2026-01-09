from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from quant_engine.utils.logger import (
    init_logging,
    get_logger,
    log_step_trace,
    to_jsonable,
)


@dataclass
class _Foo:
    a: int


class _Bar:
    def to_dict(self) -> dict[str, int]:
        return {"b": 2}


class _Baz:
    def __repr__(self) -> str:
        return "X" * 5000


def test_to_jsonable_truncates_and_serializes() -> None:
    assert to_jsonable(_Foo(1)) == {"a": 1}
    assert to_jsonable(_Bar()) == {"b": 2}
    long_repr = to_jsonable(_Baz(), max_str=10)
    assert isinstance(long_repr, str)
    assert len(long_repr) <= 10

    seq = list(range(300))
    out = to_jsonable(seq, max_items=10)
    assert out["__truncated__"] is True
    assert len(out["kept"]) == 10
    assert out["dropped"] == 290

    nested = {"a": {"b": {"c": {"d": 1}}}}
    depth_out = to_jsonable(nested, max_depth=2)
    assert "__truncated__" in depth_out["a"]["b"]


def test_log_step_trace_writes_trace_jsonl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QUANT_TRACE_FULL_MARKET", raising=False)
    config_path = tmp_path / "logging.json"
    trace_path = tmp_path / "logs" / "{run_id}" / "trace.jsonl"
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

    init_logging(config_path=str(config_path), run_id="r1", mode="default")
    logger = get_logger("tests.step_trace")

    out_path = Path(str(trace_path).format(run_id="r1", mode="default"))
    assert out_path.parent.exists()
    assert not out_path.exists()

    log_step_trace(
        logger,
        step_ts=123456,
        strategy="EXAMPLE",
        symbol="BTCUSDT",
        features={"f1": 1},
        models={"m1": {"score": 0.1}},
        portfolio={"cash": 100},
        primary_snapshots={"ohlcv": {"data_ts": 123000, "open": 1.0}},
        market_snapshots={"ohlcv": {"BTCUSDT": {"data_ts": 123000, "open": 1.0}}},
        decision_score=0.5,
        target_position=1.0,
        fills=[],
        snapshot={"timestamp": 123456, "mode": "mock"},
    )

    assert out_path.exists()
    line = out_path.read_text(encoding="utf-8").strip()
    payload = json.loads(line)

    assert payload["event"] == "engine.step.trace"
    assert payload["run_id"] == "r1"
    assert payload["mode"] == "default"
    assert payload["symbol"] == "BTCUSDT"
    assert "context" in payload
    assert payload["context"]["decision_score"] == 0.5
