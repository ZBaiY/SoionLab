from __future__ import annotations

from typing import Any

from analyze.views.schemas import (
    LIVE_PERFORMANCE_SCHEMA,
    PORTFOLIO_SUMMARY_SCHEMA,
    RECENT_TRADES_SCHEMA,
    SIGNAL_SNAPSHOT_SCHEMA,
    SYSTEM_STATUS_SCHEMA,
)


def _status_label(status: str) -> str:
    mapping = {
        "healthy": "Healthy",
        "degraded": "Degraded",
        "at_risk": "At Risk",
        "halted": "Halted",
        "unknown": "Unknown",
    }
    return mapping.get(str(status), "Unknown")


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
    session_start_ms: int,
    equity_series: list[dict[str, float]],
    series_total_available: int,
) -> dict[str, Any]:
    current_equity = float(state.get("equity") or 0.0)
    base = float(equity_series[0]["v"]) if equity_series else current_equity
    session_pnl = current_equity - base
    session_return_pct = (session_pnl / base) * 100.0 if base != 0 else 0.0
    peak = base
    if equity_series:
        peak = max(float(x["v"]) for x in equity_series)
    current_drawdown_pct = ((current_equity - peak) / peak) * 100.0 if peak != 0 else 0.0
    return {
        "schema": LIVE_PERFORMANCE_SCHEMA,
        "as_of_ms": int(state.get("as_of_ms") or 0),
        "session_start_ms": int(session_start_ms),
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
        "as_of_ms": int(state.get("as_of_ms") or 0),
        "trades": trades,
        "window_steps": int(window_steps),
        "total_fills_session": int(total_fills_session),
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

