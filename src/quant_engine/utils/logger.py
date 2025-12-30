import dataclasses
import json
import logging
import logging.config
import time
from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from logging import Logger
from pathlib import Path
from typing import Any, Optional, TypeGuard, cast

_DEFAULT_LEVEL = logging.INFO
_DEBUG_ENABLED = False
_DEBUG_MODULES: set[str] = set()
_CONFIGURED = False
_RUN_ID: str | None = None
_MODE: str | None = None

# ---------------------------------------------------------------------
# Canonical log categories (semantic contract)
# ---------------------------------------------------------------------

CATEGORY_DATA_INTEGRITY = "data_integrity"
CATEGORY_DECISION = "decision_trace"
CATEGORY_EXECUTION = "execution_discrepancy"
CATEGORY_PORTFOLIO = "portfolio_accounting"
CATEGORY_HEARTBEAT = "health_heartbeat"

def _merge_profile(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = _merge_profile(merged[k], v)
        else:
            merged[k] = v
    return merged


def _build_dict_config(profile: dict[str, Any], *, run_id: str | None, mode: str | None) -> dict[str, Any]:
    level_name = str(profile.get("level", "INFO")).upper()

    format_cfg = profile.get("format", {}) if isinstance(profile.get("format"), dict) else {}
    use_json = bool(format_cfg.get("json", True))
    formatter_name = "json" if use_json else "standard"

    handlers_cfg = profile.get("handlers", {}) if isinstance(profile.get("handlers"), dict) else {}
    console_cfg = handlers_cfg.get("console", {}) if isinstance(handlers_cfg.get("console"), dict) else {}
    file_cfg = handlers_cfg.get("file", {}) if isinstance(handlers_cfg.get("file"), dict) else {}

    handlers: dict[str, Any] = {}
    root_handlers: list[str] = []

    if bool(console_cfg.get("enabled", True)):
        handlers["console"] = {
            "class": "logging.StreamHandler",
            "level": str(console_cfg.get("level", level_name)).upper(),
            "formatter": formatter_name,
            "filters": ["context"],
            "stream": "ext://sys.stdout",
        }
        root_handlers.append("console")

    if bool(file_cfg.get("enabled", False)):
        path_template = str(file_cfg.get("path", "artifacts/logs/{mode}-{run_id}.jsonl"))
        path = Path(path_template.format(
            run_id=run_id or "run",
            mode=mode or "default",
        ))
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers["file"] = {
            "class": "logging.FileHandler",
            "level": str(file_cfg.get("level", level_name)).upper(),
            "formatter": formatter_name,
            "filters": ["context"],
            "filename": str(path),
            "encoding": "utf-8",
        }
        root_handlers.append("file")

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "context": {"()": "quant_engine.utils.logger.ContextFilter"},
        },
        "formatters": {
            "json": {"()": "quant_engine.utils.logger.JsonFormatter"},
            "standard": {
                "()": "quant_engine.utils.logger.UtcFormatter",
                "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
                "datefmt": "%Y-%m-%dT%H:%M:%S",
            },
        },
        "handlers": handlers,
        "root": {
            "level": level_name,
            "handlers": root_handlers,
        },
    }


def init_logging(
    config_path: str = "configs/logging.json",
    *,
    run_id: str | None = None,
    mode: str | None = None,
) -> None:
    global _DEFAULT_LEVEL, _DEBUG_ENABLED, _DEBUG_MODULES, _CONFIGURED, _RUN_ID, _MODE

    path = Path(config_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    with path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    profiles = cfg.get("profiles", {})
    if not isinstance(profiles, dict):
        raise TypeError("logging.json 'profiles' must be a dict")

    profile_name = str(mode or cfg.get("active_profile") or "default")
    if profile_name not in profiles:
        raise KeyError(f"logging profile not found: {profile_name}")

    base_profile = profiles.get("default", {})
    if not isinstance(base_profile, dict):
        base_profile = {}

    profile = _merge_profile(base_profile, profiles.get(profile_name, {}))

    level_name = str(profile.get("level", "INFO")).upper()
    _DEFAULT_LEVEL = getattr(logging, level_name, logging.INFO)

    debug_cfg = profile.get("debug", {})
    if not isinstance(debug_cfg, dict):
        debug_cfg = {}
    _DEBUG_ENABLED = bool(debug_cfg.get("enabled", False))
    _DEBUG_MODULES = {str(x) for x in debug_cfg.get("modules", [])}

    _RUN_ID = run_id
    _MODE = mode or profile_name

    dict_cfg = _build_dict_config(profile, run_id=run_id, mode=mode or profile_name)
    logging.config.dictConfig(dict_cfg)

    _CONFIGURED = True
    get_logger.cache_clear()
    for logger in logging.root.manager.loggerDict.values():
        if isinstance(logger, logging.Logger) and logger.level != logging.NOTSET:
            logger.setLevel(logging.NOTSET)


class ContextFilter(logging.Filter):
    """
    Guarantees LogRecord has a `context` attribute.
    This makes dynamic LogRecord extension explicit and type-safe.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        # Ensure `context` exists.
        ctx = getattr(record, "context", None)

        # If not configured yet, do not mutate records beyond ensuring the attribute exists.
        if not _CONFIGURED:
            if not hasattr(record, "context"):
                setattr(record, "context", None)
            return True

        # Normalize context to a dict so we can inject base fields.
        if ctx is None:
            ctx = {}
            setattr(record, "context", ctx)
        elif not isinstance(ctx, dict):
            ctx = {"_context": safe_jsonable(ctx)}
            setattr(record, "context", ctx)

        # Inject base context fields if missing.
        if _RUN_ID is not None and "run_id" not in ctx:
            ctx["run_id"] = _RUN_ID
        if _MODE is not None and "mode" not in ctx:
            ctx["mode"] = _MODE

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
            "logger": record.name,
            "event": record.getMessage(),
            # backward-compatible aliases (can be removed later)
            "module": record.name,
            "msg": record.getMessage(),
        }

        context = cast(Optional[dict[str, Any]], getattr(record, "context", None))

        if isinstance(context, dict) and context:
            # Lift category to top-level and avoid duplicating it inside context.
            if "category" in context:
                payload["category"] = safe_jsonable(context["category"])
                context = dict(context)
                context.pop("category", None)
            payload["context"] = safe_jsonable(context)

        try:
            return json.dumps(payload, ensure_ascii=False)
        except Exception as exc:
            fallback = {
                "ts": payload.get("ts"),
                "ts_ms": payload.get("ts_ms"),
                "level": payload.get("level"),
                "logger": payload.get("logger"),
                "event": payload.get("event"),
                "module": payload.get("module"),
                "msg": payload.get("msg"),
                "context": repr(payload.get("context")),
                "format_error": repr(exc),
            }
            return json.dumps(fallback, ensure_ascii=False)


class UtcFormatter(logging.Formatter):
    converter = time.gmtime



@lru_cache(None)
def get_logger(name: str = "quant_engine", level: int = logging.INFO) -> Logger:
    logger = logging.getLogger(name)
    if not _CONFIGURED:
        logger.setLevel(logging.NOTSET)
    return logger


def safe_jsonable(x: Any) -> Any:
    if x is None or isinstance(x, (str, int, float, bool)):
        return x
    if isinstance(x, datetime):
        if x.tzinfo is None:
            return x.replace(tzinfo=timezone.utc).isoformat()
        return x.astimezone(timezone.utc).isoformat()
    if isinstance(x, Path):
        return str(x)
    if isinstance(x, Enum):
        value = safe_jsonable(getattr(x, "value", None))
        return value if value is not None else str(x)
    if dataclasses.is_dataclass(x) and not isinstance(x, type):
        try:
            return safe_jsonable(asdict(cast(Any, x)))
        except Exception:
            return repr(x)
    if dataclasses.is_dataclass(x) and isinstance(x, type):
        # x is a dataclass class, not an instance
        return f"{x.__module__}.{x.__qualname__}"
        
    if isinstance(x, Mapping):
        out: dict[str, Any] = {}
        for k, v in x.items():
            try:
                key = safe_jsonable(k)
                if not isinstance(key, str):
                    key = repr(key)
            except Exception:
                key = repr(k)
            out[key] = safe_jsonable(v)
        return out
    if isinstance(x, (list, tuple, set)):
        return [safe_jsonable(v) for v in x]
    try:
        return str(x)
    except Exception:
        return repr(x)


def _sanitize_context(context: dict[str, Any]) -> dict[str, Any]:
    cleaned = safe_jsonable(context)
    if isinstance(cleaned, dict):
        return cleaned
    return {"_context": cleaned}


def _debug_module_matches(logger_name: str, module: str) -> bool:
    module = module.strip()
    if not module:
        return False
    if logger_name == module or logger_name.startswith(module + "."):
        return True
    parts = logger_name.split(".")
    return module in parts


def log_debug(logger: Logger, msg: str, **context):
    if not _DEBUG_ENABLED:
        return
    if _DEBUG_MODULES:
        if not any(_debug_module_matches(logger.name, m) for m in _DEBUG_MODULES):
            return
    logger.debug(msg, extra={"context": _sanitize_context(context)})


def log_info(logger: Logger, msg: str, **context):
    logger.info(msg, extra={"context": _sanitize_context(context)})


def log_warn(logger: Logger, msg: str, **context):
    logger.warning(msg, extra={"context": _sanitize_context(context)})


def log_error(logger: Logger, msg: str, **context):
    logger.error(msg, extra={"context": _sanitize_context(context)})


def log_exception(logger: Logger, msg: str, **context):
    logger.exception(msg, extra={"context": _sanitize_context(context)})

# ---------------------------------------------------------------------
# Domain-specific logging helpers (semantic, not infrastructural)
# ---------------------------------------------------------------------

def log_data_integrity(logger: Logger, msg: str, **context):
    """
    Realtime data health: gaps, missing timestamps, late arrivals.
    Expected context: data_ts, expected_ts, gap_seconds, symbol, source
    """
    context["category"] = CATEGORY_DATA_INTEGRITY
    logger.warning(msg, extra={"context": _sanitize_context(context)})


def log_portfolio(logger: Logger, msg: str, **context):
    """
    Portfolio / accounting / audit logs.
    Expected context: cash, positions, realized_pnl, unrealized_pnl, equity
    """
    context["category"] = CATEGORY_PORTFOLIO
    logger.info(msg, extra={"context": _sanitize_context(context)})


def log_decision(logger: Logger, msg: str, **context):
    """
    Decision trace logs (CRITICAL for explainability).
    Expected context: model_scores, features_hash, regime, signal, weights
    """
    context["category"] = CATEGORY_DECISION
    logger.info(msg, extra={"context": _sanitize_context(context)})


def log_execution(logger: Logger, msg: str, **context):
    """
    Execution discrepancies, slippage, partial fills, rejections.
    Expected context: order_id, expected_price, fill_price, slippage_bps
    """
    context["category"] = CATEGORY_EXECUTION
    logger.warning(msg, extra={"context": _sanitize_context(context)})


def log_heartbeat(logger: Logger, msg: str, **context):
    """
    System health / liveness logs.
    Expected context: cycle_ms, last_data_ts, backlog, component
    """
    context["category"] = CATEGORY_HEARTBEAT
    logger.info(msg, extra={"context": _sanitize_context(context)})
