from __future__ import annotations

from typing import Any

from ingestion.contracts.market import _is_market_open, _load_calendar_config, _resolve_calendar
from quant_engine.utils.logger import get_logger, log_warn


_logger = get_logger(__name__)


def market_is_closed(ts_ms: int, calendar_cfg: dict[str, Any] | None) -> bool:
    """Return True only when market is confidently known as closed.

    Conservative fallback: on any uncertainty/error this returns False
    (treat as open, so staleness checks remain active).
    """
    try:
        if not isinstance(calendar_cfg, dict) or not calendar_cfg:
            return False
        calendar_name: str | None = None
        direct = calendar_cfg.get("calendar")
        if isinstance(direct, str) and direct:
            calendar_name = direct
        elif isinstance(calendar_cfg.get("default"), str):
            calendar_name = str(calendar_cfg["default"])
        elif len(calendar_cfg) == 1:
            only_value = next(iter(calendar_cfg.values()))
            if isinstance(only_value, str) and only_value:
                calendar_name = only_value
        if not calendar_name:
            return False
        cfg = _load_calendar_config()
        if not cfg:
            log_warn(_logger, "health.session.calendar_missing", calendar=calendar_name)
            return False
        calendar = _resolve_calendar(str(calendar_name), cfg)
        return not bool(_is_market_open(int(ts_ms), calendar))
    except Exception as exc:
        log_warn(
            _logger,
            "health.session.calendar_error",
            err_type=type(exc).__name__,
            err=str(exc),
        )
        return False
