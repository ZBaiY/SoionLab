from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path

from quant_engine.utils.logger import (
    ContextFilter,
    JsonFormatter,
    _debug_module_matches,
    get_logger,
    init_logging,
    safe_jsonable,
)


class Color(Enum):
    RED = "red"


@dataclass
class Sample:
    path: Path
    created: datetime
    color: Color


def test_safe_jsonable_handles_common_types() -> None:
    payload = {
        "path": Path("foo/bar"),
        "created": datetime(2020, 1, 1, 0, 0, 0),
        "enum": Color.RED,
        "sample": Sample(Path("x/y"), datetime(2021, 1, 2, 3, 4, 5), Color.RED),
        "exc": ValueError("boom"),
        "tuple": (1, 2),
        "set": {3, 4},
    }

    out = safe_jsonable(payload)
    json.dumps(out)
    assert out["path"] == "foo/bar"
    assert "2020" in out["created"]


def test_debug_module_matching() -> None:
    assert _debug_module_matches("quant_engine.strategy.engine", "strategy")
    assert _debug_module_matches("quant_engine.strategy.engine", "quant_engine.strategy")
    assert _debug_module_matches("strategy.engine", "strategy")
    assert not _debug_module_matches("quant_engine.models", "strategy")


def test_init_logging_dictconfig_applied(tmp_path: Path) -> None:
    config = {
        "active_profile": "default",
        "profiles": {
            "default": {
                "level": "DEBUG",
                "debug": {"enabled": True, "modules": ["strategy"]},
                "handlers": {"console": {"enabled": True, "level": "DEBUG"}},
                "format": {"json": True, "timestamp_utc": True},
            }
        },
    }
    config_path = tmp_path / "logging.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    init_logging(config_path=str(config_path))
    logger = get_logger("quant_engine.test")

    assert logging.getLogger().level == logging.DEBUG
    assert logger.getEffectiveLevel() == logging.DEBUG

    root_handlers = logging.getLogger().handlers
    assert root_handlers
    assert any(isinstance(h.formatter, JsonFormatter) for h in root_handlers)
    assert any(any(isinstance(f, ContextFilter) for f in h.filters) for h in root_handlers)
