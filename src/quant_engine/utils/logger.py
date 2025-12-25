import logging
import json
from logging import Logger
from functools import lru_cache
from datetime import datetime, timezone
import os
import pathlib
from typing import Any, Optional, cast

_DEFAULT_LEVEL = logging.INFO
_DEBUG_ENABLED = False
_DEBUG_MODULES: set[str] = set()

# ---------------------------------------------------------------------
# Canonical log categories (semantic contract)
# ---------------------------------------------------------------------

CATEGORY_DATA_INTEGRITY = "data_integrity"
CATEGORY_DECISION = "decision_trace"
CATEGORY_EXECUTION = "execution_discrepancy"
CATEGORY_PORTFOLIO = "portfolio_accounting"
CATEGORY_HEARTBEAT = "health_heartbeat"

def init_logging(cfg: dict) -> None:
    global _DEFAULT_LEVEL, _DEBUG_ENABLED, _DEBUG_MODULES

    level_name = cfg.get("level", "INFO").upper()
    _DEFAULT_LEVEL = getattr(logging, level_name, logging.INFO)

    debug_cfg = cfg.get("debug", {})
    _DEBUG_ENABLED = bool(debug_cfg.get("enabled", False))
    _DEBUG_MODULES = set(debug_cfg.get("modules", []))


class ContextFilter(logging.Filter):
    """
    Guarantees LogRecord has a `context` attribute.
    This makes dynamic LogRecord extension explicit and type-safe.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "context"):
            # Explicitly inject attribute for static analyzers
            setattr(record, "context", None)
        return True
    
class JsonFormatter(logging.Formatter):
    """Structured JSON formatter for deterministic, parseable logs."""

    def format(self, record: logging.LogRecord) -> str:
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        payload: dict[str, Any] = {
            # human-readable UTC time with millisecond precision
            "ts": dt.isoformat(timespec="milliseconds"),
            # numeric epoch milliseconds for machine joins/replay
            "ts_ms": int(record.created * 1000),
            "level": record.levelname,
            "module": record.name,
            "msg": record.getMessage(),
        }

        context = cast(Optional[dict[str, Any]], getattr(record, "context", None))

        if context:
            payload["context"] = context
            if "category" in context:
                payload["category"] = context["category"]

        return json.dumps(payload, ensure_ascii=False)



@lru_cache(None)
def get_logger(name: str = "quant_engine", level: int = logging.INFO) -> Logger:
    logger = logging.getLogger(name)
    logger.setLevel(_DEFAULT_LEVEL)

    if logger.handlers: 
        return logger

    handler = logging.StreamHandler()
    handler.addFilter(ContextFilter())
    handler.setFormatter(JsonFormatter())

    logger.addHandler(handler)
    return logger


def log_debug(logger: Logger, msg: str, **context):
    module_name = logger.name.split(".")[0]
    if not _DEBUG_ENABLED:
        return
    if _DEBUG_MODULES and (module_name not in _DEBUG_MODULES):
        return
    logger.debug(msg, extra={"context": context})


def log_info(logger: Logger, msg: str, **context):
    logger.info(msg, extra={"context": context})


def log_warn(logger: Logger, msg: str, **context):
    logger.warning(msg, extra={"context": context})


def log_error(logger: Logger, msg: str, **context):
    logger.error(msg, extra={"context": context})

# ---------------------------------------------------------------------
# Domain-specific logging helpers (semantic, not infrastructural)
# ---------------------------------------------------------------------

def log_data_integrity(logger: Logger, msg: str, **context):
    """
    Realtime data health: gaps, missing timestamps, late arrivals.
    Expected context: data_ts, expected_ts, gap_seconds, symbol, source
    """
    context["category"] = CATEGORY_DATA_INTEGRITY
    logger.warning(msg, extra={"context": context})


def log_portfolio(logger: Logger, msg: str, **context):
    """
    Portfolio / accounting / audit logs.
    Expected context: cash, positions, realized_pnl, unrealized_pnl, equity
    """
    context["category"] = CATEGORY_PORTFOLIO
    logger.info(msg, extra={"context": context})


def log_decision(logger: Logger, msg: str, **context):
    """
    Decision trace logs (CRITICAL for explainability).
    Expected context: model_scores, features_hash, regime, signal, weights
    """
    context["category"] = CATEGORY_DECISION
    logger.info(msg, extra={"context": context})


def log_execution(logger: Logger, msg: str, **context):
    """
    Execution discrepancies, slippage, partial fills, rejections.
    Expected context: order_id, expected_price, fill_price, slippage_bps
    """
    context["category"] = CATEGORY_EXECUTION
    logger.warning(msg, extra={"context": context})


def log_heartbeat(logger: Logger, msg: str, **context):
    """
    System health / liveness logs.
    Expected context: cycle_ms, last_data_ts, backlog, component
    """
    context["category"] = CATEGORY_HEARTBEAT
    logger.info(msg, extra={"context": context})