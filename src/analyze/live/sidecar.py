from __future__ import annotations

import json
import time
from collections import deque
from pathlib import Path
from typing import Any

from analyze.live.normalizer import normalize_live_state, to_iso_utc
from analyze.views.live_views import (
    build_live_performance,
    build_portfolio_summary,
    build_recent_trades,
    build_signal_snapshot,
    build_system_status,
)


class LiveSidecarState:
    """In-memory normalized state with bounded history windows."""

    def __init__(
        self,
        *,
        run_id: str,
        strategy: str,
        symbol: str,
        interval: str,
        equity_window_steps: int = 50,
        trades_window: int = 20,
    ) -> None:
        self.run_id = str(run_id)
        self.strategy = str(strategy)
        self.symbol = str(symbol)
        self.interval = str(interval)
        self.uptime_since_ms = int(time.time() * 1000)
        self.session_start_ms = self.uptime_since_ms
        self._equity: deque[dict[str, float]] = deque(maxlen=max(1, int(equity_window_steps)))
        self._trades: deque[dict[str, Any]] = deque(maxlen=max(1, int(trades_window)))
        self._raw: dict[str, Any] = {}
        self._state: dict[str, Any] = {}
        self._steps_completed = 0
        self._fills_total = 0

    def ingest_raw(self, raw_state: dict[str, Any], *, now_ms: int | None = None) -> None:
        now = int(now_ms) if now_ms is not None else int(time.time() * 1000)
        normalized = normalize_live_state(raw_state, now_ms=now)
        self._raw = dict(raw_state)
        self._state = dict(normalized)
        self._steps_completed += 1
        self._equity.append({"t": int(normalized.get("as_of_ms") or now), "v": float(normalized.get("equity") or 0.0)})
        fills = list(normalized.get("fills") or [])
        self._fills_total += len(fills)
        for fill in fills:
            self._trades.append(fill)
        self._state["steps_completed"] = self._steps_completed
        self._state["uptime_since"] = to_iso_utc(self.uptime_since_ms)

    def api_payload(self, path: str, *, now_ms: int | None = None) -> dict[str, Any]:
        now = int(now_ms) if now_ms is not None else int(time.time() * 1000)
        if path == "/api/raw-state":
            return dict(self._raw)
        if not self._state:
            return {"error": "state_unavailable"}
        if path == "/api/status":
            return build_system_status(
                self._state,
                now_ms=now,
                run_id=self.run_id,
                strategy=self.strategy,
                symbol=self.symbol,
                interval=self.interval,
                uptime_since_ms=self.uptime_since_ms,
            )
        if path == "/api/portfolio":
            return build_portfolio_summary(self._state)
        if path == "/api/performance":
            return build_live_performance(
                self._state,
                session_start_ms=self.session_start_ms,
                equity_series=list(self._equity),
                series_total_available=self._steps_completed,
            )
        if path == "/api/trades":
            return build_recent_trades(
                self._state,
                trades=list(self._trades),
                window_steps=len(self._equity),
                total_fills_session=self._fills_total,
            )
        if path == "/api/signal":
            return build_signal_snapshot(self._state)
        return {"error": "not_found"}


def read_current_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("current.json must contain a JSON object")
    return data
