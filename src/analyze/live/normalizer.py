from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def to_iso_utc(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def parse_domain_key(raw_key: str) -> tuple[str, str]:
    parts = str(raw_key).split(":")
    if len(parts) >= 2:
        return parts[0], parts[1]
    if parts:
        return parts[0], ""
    return "", ""


def map_health_mode(value: str) -> str:
    mode = str(value or "").lower()
    if mode == "running":
        return "healthy"
    if mode == "degraded":
        return "degraded"
    if mode in {"safe_hold", "safe_flatten"}:
        return "at_risk"
    if mode == "halt":
        return "halted"
    return "unknown"


def map_execution_permit(value: str) -> str:
    permit = str(value or "").lower()
    if permit == "full":
        return "full"
    if permit == "reduce_only":
        return "reduce_only"
    if permit == "block":
        return "block"
    return "block"


def _unwrap_portfolio(raw_portfolio: Any) -> dict[str, Any]:
    if isinstance(raw_portfolio, dict):
        wrapped = raw_portfolio.get("snapshot_dict")
        if isinstance(wrapped, dict):
            return dict(wrapped)
        return dict(raw_portfolio)
    return {}


def normalize_live_state(raw_state: dict[str, Any], *, now_ms: int) -> dict[str, Any]:
    ts_ms = int(raw_state.get("timestamp") or now_ms)
    health = dict(raw_state.get("health") or {})
    portfolio = _unwrap_portfolio(raw_state.get("portfolio"))
    positions_in = portfolio.get("positions")
    positions: list[dict[str, Any]] = []
    if isinstance(positions_in, dict):
        for symbol, pos in positions_in.items():
            if not isinstance(pos, dict):
                continue
            lots = int(pos.get("lots") or 0)
            qty = abs(float(pos.get("qty") or 0.0))
            if qty == 0.0:
                continue
            entry = float(pos.get("entry_price") or 0.0)
            unrealized = float(pos.get("unrealized_pnl") or 0.0)
            denom = entry * qty
            unrealized_pct = (unrealized / denom) * 100.0 if denom > 0 else 0.0
            positions.append(
                {
                    "symbol": str(symbol),
                    "side": "LONG" if float(pos.get("qty") or 0.0) >= 0 else "SHORT",
                    "quantity": qty,
                    "lots": lots,
                    "entry_price": entry,
                    "current_value": (entry * qty) + unrealized,
                    "unrealized_pnl": unrealized,
                    "unrealized_pnl_pct": unrealized_pct,
                }
            )

    data_feeds: list[dict[str, Any]] = []
    for raw_key, val in dict(health.get("domains") or {}).items():
        if not isinstance(val, dict):
            continue
        domain, symbol = parse_domain_key(str(raw_key))
        data_feeds.append(
            {
                "domain": domain,
                "symbol": symbol,
                "state": str(val.get("state") or "unknown"),
                "staleness_ms": val.get("staleness_ms"),
                "fault_count": int(val.get("fault_count") or 0),
            }
        )

    fills_in = list(raw_state.get("fills") or [])
    fills: list[dict[str, Any]] = []
    for fill in fills_in:
        if not isinstance(fill, dict):
            continue
        side = str(fill.get("side") or "").upper()
        qty = abs(float(fill.get("filled_qty") or 0.0))
        fills.append(
            {
                "time": to_iso_utc(ts_ms),
                "time_ms": ts_ms,
                "symbol": str(fill.get("symbol") or portfolio.get("symbol") or ""),
                "side": side if side in {"BUY", "SELL"} else ("BUY" if float(fill.get("filled_qty") or 0.0) >= 0 else "SELL"),
                "quantity": qty,
                "price": float(fill.get("fill_price") or 0.0),
                "fee": float(fill.get("fee") or 0.0),
                "source": "live_binance" if fill.get("exchange") else "simulated",
            }
        )

    equity = float(portfolio.get("total_equity") or 0.0)
    status = map_health_mode(str(health.get("global_mode") or ""))
    return {
        "as_of_ms": ts_ms,
        "as_of": to_iso_utc(ts_ms),
        "status": status,
        "execution_permit": map_execution_permit(str(health.get("execution_permit") or "")),
        "consecutive_skips": int(health.get("consecutive_skips") or 0),
        "consecutive_failures": int(health.get("consecutive_step_failures") or 0),
        "last_step_ts": health.get("last_step_ts"),
        "data_feeds": data_feeds,
        "portfolio": {
            "cash": float(portfolio.get("cash") or 0.0),
            "total_equity": equity,
            "realized_pnl": float(portfolio.get("realized_pnl") or 0.0),
            "unrealized_pnl": float(portfolio.get("unrealized_pnl") or 0.0),
            "gross_exposure": float(portfolio.get("exposure") or 0.0),
            "leverage": float(portfolio.get("leverage") or 0.0),
            "leverage_pct": float(portfolio.get("leverage") or 0.0) * 100.0,
            "positions": positions,
        },
        "signal": {
            "decision_score": raw_state.get("decision_score"),
            "target_position": raw_state.get("target_position"),
            "features": dict(raw_state.get("features") or {}),
            "models": dict(raw_state.get("model_outputs") or {}),
        },
        "fills": fills,
        "equity": equity,
    }
