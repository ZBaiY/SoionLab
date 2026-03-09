from __future__ import annotations

from typing import Any

from analyze.live.normalizer import to_iso_utc
from analyze.views.schemas import (
    FRESHNESS_METADATA_SCHEMA,
    LIVE_PERFORMANCE_SCHEMA,
    PORTFOLIO_SUMMARY_SCHEMA,
    RECENT_TRADES_SCHEMA,
    SIGNAL_SNAPSHOT_SCHEMA,
    SYSTEM_STATUS_SCHEMA,
)


_STALE_AGE_MS = 30_000


def _status_label(status: str) -> str:
    mapping = {
        "healthy": "Healthy",
        "degraded": "Degraded",
        "at_risk": "At Risk",
        "halted": "Halted",
        "unknown": "Unknown",
    }
    return mapping.get(str(status), "Unknown")


def _snapshot_age_label(freshness_ms: int) -> str:
    if freshness_ms < 1_000:
        return "fresh"
    if freshness_ms < 10_000:
        return "recent"
    if freshness_ms < _STALE_AGE_MS:
        return "aging"
    return "stale"


def build_system_status(
    state: dict[str, Any], *, now_ms: int, run_id: str, strategy: str, symbol: str, interval: str, uptime_since_ms: int
) -> dict[str, Any]:
    as_of_ms = int(state.get("as_of_ms") or now_ms)
    return {
        "schema": SYSTEM_STATUS_SCHEMA,
        "as_of": state.get("as_of"),
        "as_of_ms": as_of_ms,
        "freshness_ms": max(0, int(now_ms) - as_of_ms),
        "run_id": run_id,
        "strategy": strategy,
        "symbol": symbol,
        "interval": interval,
        "engine_mode": "realtime",
        "status": state.get("status"),
        "status_label": _status_label(str(state.get("status") or "unknown")),
        "execution_permit": state.get("execution_permit"),
        "uptime_since": state.get("uptime_since"),
        "uptime_since_ms": int(uptime_since_ms),
        "steps_completed": int(state.get("steps_completed") or 0),
        "consecutive_skips": int(state.get("consecutive_skips") or 0),
        "consecutive_failures": int(state.get("consecutive_failures") or 0),
        "data_feeds": list(state.get("data_feeds") or []),
    }


def build_portfolio_summary(state: dict[str, Any]) -> dict[str, Any]:
    portfolio = dict(state.get("portfolio") or {})
    return {
        "schema": PORTFOLIO_SUMMARY_SCHEMA,
        "as_of": state.get("as_of"),
        "as_of_ms": int(state.get("as_of_ms") or 0),
        "cash": float(portfolio.get("cash") or 0.0),
        "total_equity": float(portfolio.get("total_equity") or 0.0),
        "realized_pnl": float(portfolio.get("realized_pnl") or 0.0),
        "unrealized_pnl": float(portfolio.get("unrealized_pnl") or 0.0),
        "gross_exposure": float(portfolio.get("gross_exposure") or 0.0),
        "leverage": float(portfolio.get("leverage") or 0.0),
        "leverage_pct": float(portfolio.get("leverage_pct") or 0.0),
        "positions": list(portfolio.get("positions") or []),
    }


def build_live_performance(
    state: dict[str, Any],
    *,
    session_start: str | None,
    session_start_ms: int,
    equity_series: list[dict[str, Any]],
    series_total_available: int,
) -> dict[str, Any]:
    current_equity = float(state.get("equity") or 0.0)
    base = float(equity_series[0]["equity"]) if equity_series else current_equity
    session_pnl = current_equity - base
    session_return_pct = (session_pnl / base) * 100.0 if base != 0 else 0.0
    peak = base
    if equity_series:
        peak = max(float(x["equity"]) for x in equity_series)
    current_drawdown_pct = ((current_equity - peak) / peak) * 100.0 if peak != 0 else 0.0
    return {
        "schema": LIVE_PERFORMANCE_SCHEMA,
        "as_of": state.get("as_of"),
        "as_of_ms": int(state.get("as_of_ms") or 0),
        "session_start": session_start,
        "session_start_ms": int(session_start_ms),
        "current_equity": current_equity,
        "session_pnl": session_pnl,
        "session_return_pct": session_return_pct,
        "current_drawdown_pct": current_drawdown_pct,
        "peak_equity": peak,
        "equity_series": equity_series,
        "series_window_steps": len(equity_series),
        "series_total_available": int(series_total_available),
    }


def build_recent_trades(state: dict[str, Any], *, trades: list[dict[str, Any]], window_steps: int, total_fills_session: int) -> dict[str, Any]:
    return {
        "schema": RECENT_TRADES_SCHEMA,
        "as_of": state.get("as_of"),
        "as_of_ms": int(state.get("as_of_ms") or 0),
        "trades": trades,
        "window_steps": int(window_steps),
        "total_fills_session": int(total_fills_session),
    }


def build_freshness_metadata(state: dict[str, Any], *, now_ms: int) -> dict[str, Any]:
    as_of_ms = int(state.get("as_of_ms") or now_ms)
    last_step_ts_ms = int(state.get("last_step_ts") or as_of_ms)
    freshness_ms = max(0, int(now_ms) - as_of_ms)
    last_step_age_ms = max(0, int(now_ms) - last_step_ts_ms)
    feeds = [
        {
            "domain": str(feed.get("domain") or ""),
            "symbol": str(feed.get("symbol") or ""),
            "staleness_ms": feed.get("staleness_ms"),
            "state": str(feed.get("state") or "unknown"),
        }
        for feed in list(state.get("data_feeds") or [])
    ]
    unhealthy_feed = any(feed["state"] != "healthy" for feed in feeds)
    stale_feed = any(
        isinstance(feed.get("staleness_ms"), (int, float)) and int(feed["staleness_ms"]) >= _STALE_AGE_MS for feed in feeds
    )
    return {
        "schema": FRESHNESS_METADATA_SCHEMA,
        "as_of": state.get("as_of"),
        "as_of_ms": as_of_ms,
        "freshness_ms": freshness_ms,
        "snapshot_age_label": _snapshot_age_label(freshness_ms),
        "is_stale": bool(freshness_ms >= _STALE_AGE_MS or last_step_age_ms >= _STALE_AGE_MS or unhealthy_feed or stale_feed),
        "last_step_ts": to_iso_utc(last_step_ts_ms),
        "last_step_ts_ms": last_step_ts_ms,
        "last_step_age_ms": last_step_age_ms,
        "feeds": feeds,
    }


def build_signal_snapshot(state: dict[str, Any]) -> dict[str, Any]:
    signal = dict(state.get("signal") or {})
    return {
        "schema": SIGNAL_SNAPSHOT_SCHEMA,
        "as_of_ms": int(state.get("as_of_ms") or 0),
        "decision_score": signal.get("decision_score"),
        "target_position": signal.get("target_position"),
        "features": dict(signal.get("features") or {}),
        "models": dict(signal.get("models") or {}),
        "market_context": {},
    }
