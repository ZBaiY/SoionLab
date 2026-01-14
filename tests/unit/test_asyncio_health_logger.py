from __future__ import annotations

import asyncio
import json

import pytest

from quant_engine.utils.asyncio_health import start_asyncio_heartbeat
from quant_engine.utils.logger import init_logging


@pytest.mark.asyncio
async def test_asyncio_heartbeat_writes_jsonl(tmp_path):
    cfg = {
        "active_profile": "default",
        "profiles": {
            "default": {
                "level": "INFO",
                "format": {"json": True},
                "handlers": {
                    "console": {"enabled": False},
                    "file": {
                        "enabled": True,
                        "level": "INFO",
                        "path": str(tmp_path / "runs/{run_id}/logs/{mode}.jsonl"),
                    },
                    "trace": {
                        "enabled": True,
                        "level": "INFO",
                        "path": str(tmp_path / "runs/{run_id}/logs/trace.jsonl"),
                    },
                    "asyncio": {
                        "enabled": True,
                        "level": "INFO",
                        "path": str(tmp_path / "runs/{run_id}/logs/asyncio.jsonl"),
                    },
                },
                "debug": {"enabled": False, "modules": []},
            }
        },
    }
    cfg_path = tmp_path / "logging.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
    init_logging(config_path=str(cfg_path), run_id="test", mode="default")

    task = start_asyncio_heartbeat(enabled=True, period_s=0.05)
    assert task is not None
    await asyncio.sleep(0.2)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    logs_dir = tmp_path / "runs" / "test" / "logs"
    default_log = logs_dir / "default.jsonl"
    trace_log = logs_dir / "trace.jsonl"
    asyncio_log = logs_dir / "asyncio.jsonl"
    assert default_log.exists()
    assert asyncio_log.exists()
    assert asyncio_log.parent == default_log.parent
    assert asyncio_log.parent == trace_log.parent

    lines = [line for line in asyncio_log.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) >= 2
    parsed = [json.loads(line) for line in lines]
    assert any(item.get("event") == "asyncio.health" for item in parsed)
    assert all(
        "loop_lag_ms" in item.get("context", {}) for item in parsed if item.get("event") == "asyncio.health"
    )
