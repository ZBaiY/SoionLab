from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from analyze.views.backtest_views import (
    build_backtest_card,
    build_drawdown_chart,
    build_equity_chart,
    build_exposure_chart,
    build_performance_summary,
    build_pnl_period_chart,
    build_quality_badge,
    build_trade_list,
    build_trade_pnl_histogram,
    build_warning_list,
)
from analyze.views.schemas import REPORT_SCHEMA_VERSION, RUN_INDEX_SCHEMA


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return float(default)


_TRADE_RECON_QTY_EPS = 1e-12
_TRADE_RECON_NOTIONAL_EPS = 1e-6


def _find_trace_path(run_dir: Path) -> Path:
    logs = run_dir / "logs"
    direct = logs / "trace.jsonl"
    if direct.exists():
        return direct
    for p in logs.glob("*/trace.jsonl"):
        return p
    raise FileNotFoundError(f"trace.jsonl not found under {logs}")


def _warning(
    wid: str,
    *,
    category: str,
    severity: str,
    title: str,
    description: str,
    evidence: dict[str, Any] | None = None,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> dict[str, Any]:
    return {
        "id": wid,
        "category": category,
        "severity": severity,
        "title": title,
        "description": description,
        "evidence": dict(evidence or {}),
        "affected_range": {"start_ts_ms": start_ms, "end_ts_ms": end_ms},
    }


def _compute_verdict(warnings: list[dict[str, Any]]) -> str:
    if any(str(w.get("severity")) == "critical" for w in warnings):
        return "INVALID"
    warning_count = sum(1 for w in warnings if str(w.get("severity")) == "warning")
    if warning_count > 5:
        return "DEGRADED"
    if warning_count > 0:
        return "QUALIFIED"
    return "CLEAN"


def _build_round_trip_trades(fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # FIFO lot matching for deterministic round-trip reconstruction from signed fills.
    opens: list[dict[str, Any]] = []
    out: list[dict[str, Any]] = []
    trade_id = 1
    for fill in fills:
        qty_signed = _safe_float(fill.get("filled_qty"))
        if abs(qty_signed) <= _TRADE_RECON_QTY_EPS:
            continue
        side = "BUY" if qty_signed > 0 else "SELL"
        qty = abs(qty_signed)
        px = _safe_float(fill.get("fill_price"))
        fee = _safe_float(fill.get("fee"))
        ts_ms = int(fill.get("_ts_ms") or 0)
        symbol = str(fill.get("symbol") or "")
        if not opens:
            opens.append({"qty": qty, "side": side, "price": px, "time_ms": ts_ms, "fee": fee, "symbol": symbol, "step_idx": int(fill.get("_step_idx") or 0)})
            continue
        remaining = qty
        while remaining > _TRADE_RECON_QTY_EPS and opens:
            entry = opens[0]
            if float(entry["qty"]) <= _TRADE_RECON_QTY_EPS:
                opens.pop(0)
                continue
            take = min(remaining, float(entry["qty"]))
            if take <= _TRADE_RECON_QTY_EPS:
                break
            entry_qty = float(entry["qty"])
            entry_side = str(entry["side"])
            pnl = ((px - float(entry["price"])) * take) if entry_side == "BUY" else ((float(entry["price"]) - px) * take)
            fees = float(entry.get("fee") or 0.0) + fee
            gross_notional = float(entry["price"]) * take
            ret_pct = (pnl / gross_notional) * 100.0 if gross_notional > 0 else 0.0
            if gross_notional > _TRADE_RECON_NOTIONAL_EPS:
                out.append(
                    {
                        "trade_id": trade_id,
                        "symbol": symbol or str(entry.get("symbol") or ""),
                        "side": entry_side,
                        "entry_time": datetime.fromtimestamp(int(entry["time_ms"]) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                        "entry_time_ms": int(entry["time_ms"]),
                        "entry_price": float(entry["price"]),
                        "exit_time": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                        "exit_time_ms": ts_ms,
                        "exit_price": px,
                        "quantity": take,
                        "realized_pnl": pnl,
                        "fees": fees,
                        "net_pnl": pnl - fees,
                        "holding_steps": max(0, int(fill.get("_step_idx") or 0) - int(entry.get("step_idx") or 0)),
                        "return_pct": ret_pct,
                        "outcome": "WIN" if pnl >= 0.0 else "LOSS",
                    }
                )
                trade_id += 1
            remaining -= take
            if remaining <= _TRADE_RECON_QTY_EPS:
                remaining = 0.0
            new_entry_qty = entry_qty - take
            if abs(new_entry_qty) <= _TRADE_RECON_QTY_EPS or take >= entry_qty:
                opens.pop(0)
            else:
                entry["qty"] = new_entry_qty
        if remaining > _TRADE_RECON_QTY_EPS:
            opens.append({"qty": remaining, "side": side, "price": px, "time_ms": ts_ms, "fee": fee, "symbol": symbol, "step_idx": int(fill.get("_step_idx") or 0)})
    return out


def build_report_from_trace(*, trace_path: Path, run_id: str) -> dict[str, Any]:
    header: dict[str, Any] | None = None
    steps: list[dict[str, Any]] = []
    corrupt_lines = 0
    with trace_path.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                corrupt_lines += 1
                continue
            if not isinstance(obj, dict):
                continue
            event = obj.get("event")
            if event == "trace.header":
                header = obj
            elif event == "engine.step.trace":
                steps.append(obj)

    warnings: list[dict[str, Any]] = []
    if header is None:
        warnings.append(
            _warning(
                "missing_trace_header",
                category="completeness",
                severity="critical",
                title="Missing trace header",
                description="trace.header event was not found",
            )
        )
        header = {}
    if not steps:
        warnings.append(
            _warning(
                "empty_run",
                category="completeness",
                severity="critical",
                title="Empty run",
                description="No engine.step.trace records were found",
            )
        )

    ts_list: list[int] = []
    equity_curve: list[list[float]] = []
    exposure_curve: list[list[float]] = []
    fills: list[dict[str, Any]] = []
    rejection_count = 0
    clamp_count = 0
    fill_side_counts = {"BUY": 0, "SELL": 0}
    total_turnover = 0.0
    for step_idx, step in enumerate(steps):
        ts_ms = int(step.get("ts_ms") or 0)
        ts_list.append(ts_ms)
        portfolio = dict(step.get("portfolio") or {})
        portfolio_snap = dict(portfolio.get("snapshot_dict") or {})
        equity = _safe_float(portfolio_snap.get("total_equity"))
        exposure = _safe_float(portfolio_snap.get("exposure"))
        equity_curve.append([float(ts_ms), equity])
        exposure_curve.append([float(ts_ms), exposure])
        cash = _safe_float(portfolio_snap.get("cash"))
        if cash < 0:
            warnings.append(
                _warning(
                    f"negative_cash_{step_idx}",
                    category="accounting",
                    severity="critical",
                    title="Negative cash detected",
                    description="Cash became negative during backtest",
                    evidence={"cash": cash},
                    start_ms=ts_ms,
                    end_ms=ts_ms,
                )
            )
        for v in (portfolio_snap.get("cash"), portfolio_snap.get("total_equity"), portfolio_snap.get("realized_pnl"), portfolio_snap.get("unrealized_pnl")):
            vv = _safe_float(v)
            if not math.isfinite(vv):
                warnings.append(
                    _warning(
                        f"nan_inf_portfolio_{step_idx}",
                        category="accounting",
                        severity="critical",
                        title="NaN/Inf portfolio field",
                        description="Non-finite value detected in portfolio snapshot",
                        start_ms=ts_ms,
                        end_ms=ts_ms,
                    )
                )
                break
        if bool(step.get("fills")) and not bool(step.get("closed_bar_ready", True)):
            warnings.append(
                _warning(
                    f"lookahead_fill_{step_idx}",
                    category="lookahead",
                    severity="critical",
                    title="Fill on unready closed bar",
                    description="Fill occurred while closed_bar_ready=false",
                    start_ms=ts_ms,
                    end_ms=ts_ms,
                )
            )
        ev = step.get("expected_visible_end_ts")
        av = step.get("actual_last_ts")
        if isinstance(ev, int) and isinstance(av, int) and ev != av:
            warnings.append(
                _warning(
                    f"lookahead_mismatch_{step_idx}",
                    category="lookahead",
                    severity="warning",
                    title="Timestamp visibility mismatch",
                    description="expected_visible_end_ts differed from actual_last_ts",
                    evidence={"expected": ev, "actual": av},
                    start_ms=ts_ms,
                    end_ms=ts_ms,
                )
            )
        for fill in list(step.get("fills") or []):
            if not isinstance(fill, dict):
                continue
            fill_row = dict(fill)
            fill_row["_ts_ms"] = ts_ms
            fill_row["_step_idx"] = step_idx
            fills.append(fill_row)
            side = str(fill_row.get("side") or "").upper()
            if side in fill_side_counts:
                fill_side_counts[side] += 1
            qty = abs(_safe_float(fill_row.get("filled_qty")))
            px = _safe_float(fill_row.get("fill_price"))
            total_turnover += qty * px
            decision = str(fill_row.get("execution_decision") or "")
            if decision == "REJECTED":
                rejection_count += 1
            elif decision == "CLAMPED":
                clamp_count += 1
        market_snapshots = dict(step.get("market_snapshots") or {})
        for domain_payload in market_snapshots.values():
            if not isinstance(domain_payload, dict):
                continue
            for snap in domain_payload.values():
                if not isinstance(snap, dict):
                    continue
                numeric = snap.get("numeric")
                if isinstance(numeric, dict):
                    data_ts = numeric.get("data_ts")
                    if isinstance(data_ts, int):
                        lag = int(ts_ms) - int(data_ts)
                        interval_ms = 60_000
                        interval = str(header.get("interval") or "")
                        if interval.endswith("m") and interval[:-1].isdigit():
                            interval_ms = int(interval[:-1]) * 60_000
                        if lag > 2 * interval_ms:
                            warnings.append(
                                _warning(
                                    f"stale_data_{step_idx}",
                                    category="data_integrity",
                                    severity="warning",
                                    title="Stale market data",
                                    description="Snapshot data timestamp lag exceeded 2x interval",
                                    evidence={"lag_ms": lag},
                                    start_ms=ts_ms,
                                    end_ms=ts_ms,
                                )
                            )

    duplicate_ts = len(ts_list) - len(set(ts_list))
    if duplicate_ts > 0:
        warnings.append(
            _warning(
                "duplicate_timestamps",
                category="data_integrity",
                severity="warning",
                title="Duplicate step timestamps",
                description=f"Detected {duplicate_ts} duplicate timestamps",
            )
        )
    non_monotonic = sum(1 for i in range(1, len(ts_list)) if ts_list[i] <= ts_list[i - 1])
    if non_monotonic > 0:
        warnings.append(
            _warning(
                "non_monotonic_timestamps",
                category="data_integrity",
                severity="critical",
                title="Non-monotonic step timestamps",
                description=f"Detected {non_monotonic} non-monotonic step transitions",
            )
        )
    missing_bar_count = 0
    interval_ms = 60_000
    interval = str(header.get("interval") or "")
    if interval.endswith("m") and interval[:-1].isdigit():
        interval_ms = int(interval[:-1]) * 60_000
    for i in range(1, len(ts_list)):
        gap = ts_list[i] - ts_list[i - 1]
        if gap > interval_ms:
            missing_bar_count += int(gap // interval_ms) - 1
    if missing_bar_count > 0:
        warnings.append(
            _warning(
                "missing_bars",
                category="data_integrity",
                severity="warning",
                title="Missing bars detected",
                description=f"Detected {missing_bar_count} missing bars in step timeline",
            )
        )

    if corrupt_lines > 0:
        warnings.append(
            _warning(
                "corrupt_lines",
                category="completeness",
                severity="info",
                title="Corrupt trace lines skipped",
                description=f"Skipped {corrupt_lines} corrupt trace lines",
            )
        )

    start_ms = int(header.get("start_ts_ms") or (ts_list[0] if ts_list else 0))
    end_ms = int(ts_list[-1]) if ts_list else start_ms
    duration_ms = max(0, end_ms - start_ms)
    initial_equity = float(equity_curve[0][1]) if equity_curve else 0.0
    final_equity = float(equity_curve[-1][1]) if equity_curve else 0.0
    total_return = ((final_equity - initial_equity) / initial_equity) if initial_equity != 0 else 0.0
    steps_per_year = 31_536_000_000.0 / float(interval_ms) if interval_ms > 0 else 0.0
    returns: list[float] = []
    for i in range(1, len(equity_curve)):
        prev = float(equity_curve[i - 1][1])
        cur = float(equity_curve[i][1])
        returns.append(((cur - prev) / prev) if prev != 0 else 0.0)
    mean_ret = sum(returns) / len(returns) if returns else 0.0
    variance = sum((r - mean_ret) ** 2 for r in returns) / len(returns) if returns else 0.0
    std_ret = math.sqrt(variance)
    downside_var = sum((min(0.0, r)) ** 2 for r in returns) / len(returns) if returns else 0.0
    downside = math.sqrt(downside_var)
    annualization = math.sqrt(steps_per_year) if steps_per_year > 0 else 0.0
    sharpe = (mean_ret / std_ret) * annualization if std_ret > 0 else 0.0
    sortino = (mean_ret / downside) * annualization if downside > 0 else 0.0
    annualized_return = 0.0
    if duration_ms > 0 and initial_equity > 0:
        years = 31_536_000_000.0 / float(duration_ms)
        try:
            annualized_return = ((1.0 + total_return) ** years) - 1.0
        except OverflowError:
            annualized_return = 0.0

    peak = None
    max_drawdown = 0.0
    max_dd_dur = 0
    cur_dd_dur = 0
    for _, eq in equity_curve:
        peak = float(eq) if peak is None else max(peak, float(eq))
        if peak and float(eq) < peak:
            cur_dd_dur += 1
            dd = (float(eq) - peak) / peak
            max_drawdown = min(max_drawdown, dd)
            max_dd_dur = max(max_dd_dur, cur_dd_dur)
        else:
            cur_dd_dur = 0
    calmar = (annualized_return / abs(max_drawdown)) if max_drawdown < 0 else 0.0

    trade_round_trips = _build_round_trip_trades(fills)
    wins = [t for t in trade_round_trips if float(t.get("realized_pnl") or 0.0) > 0.0]
    losses = [t for t in trade_round_trips if float(t.get("realized_pnl") or 0.0) < 0.0]
    gross_profit = sum(float(t.get("realized_pnl") or 0.0) for t in wins)
    gross_loss = abs(sum(float(t.get("realized_pnl") or 0.0) for t in losses))
    warning_counts = {
        "critical": sum(1 for w in warnings if str(w.get("severity")) == "critical"),
        "warning": sum(1 for w in warnings if str(w.get("severity")) == "warning"),
        "info": sum(1 for w in warnings if str(w.get("severity")) == "info"),
    }
    verdict = _compute_verdict(warnings)

    last_portfolio = {}
    if steps:
        last_portfolio = dict((dict(steps[-1].get("portfolio") or {})).get("snapshot_dict") or {})
    report = {
        "report_schema_version": REPORT_SCHEMA_VERSION,
        "report_generated_at": _now_iso(),
        "run_metadata": {
            "run_id": run_id,
            "engine_mode": str(header.get("engine_mode") or "BACKTEST"),
            "strategy_name": str(header.get("strategy_name") or ""),
            "symbol": str(steps[-1].get("symbol") if steps else ""),
            "interval": str(header.get("interval") or ""),
            "start_ts_ms": start_ms,
            "end_ts_ms": end_ms,
            "total_steps": len(steps),
            "config_hash": header.get("config_hash"),
            "git_sha": header.get("engine_git_sha"),
            "execution_constraints": header.get("execution_constraints") or {},
        },
        "performance": {
            "total_return": total_return,
            "annualized_return": annualized_return,
            "max_drawdown": max_drawdown,
            "max_drawdown_duration_steps": int(max_dd_dur),
            "sharpe_ratio": sharpe,
            "sortino_ratio": sortino,
            "calmar_ratio": calmar,
            "final_equity": final_equity,
            "final_realized_pnl": _safe_float(last_portfolio.get("realized_pnl")),
            "max_exposure": max((_safe_float((dict(s.get("portfolio") or {}).get("snapshot_dict") or {}).get("exposure")) for s in steps), default=0.0),
            "max_leverage": max((_safe_float((dict(s.get("portfolio") or {}).get("snapshot_dict") or {}).get("leverage")) for s in steps), default=0.0),
        },
        "trades": {
            "total_trades": len(trade_round_trips),
            "total_fills": len(fills),
            "win_rate": (len(wins) / len(trade_round_trips)) if trade_round_trips else 0.0,
            "avg_win": (gross_profit / len(wins)) if wins else 0.0,
            "avg_loss": (sum(float(t.get("realized_pnl") or 0.0) for t in losses) / len(losses)) if losses else 0.0,
            "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else 0.0,
            "avg_holding_period_steps": int(sum(int(t.get("holding_steps") or 0) for t in trade_round_trips) / len(trade_round_trips)) if trade_round_trips else 0,
            "max_consecutive_wins": 0,
            "max_consecutive_losses": 0,
            "rejection_count": int(rejection_count),
            "clamp_count": int(clamp_count),
            "turnover": total_turnover / initial_equity if initial_equity > 0 else 0.0,
            "fill_side_counts": fill_side_counts,
        },
        "quality": {
            "verdict": verdict,
            "warning_counts": warning_counts,
            "warnings": warnings,
            "corrupt_lines_skipped": int(corrupt_lines),
        },
        "equity_curve": equity_curve,
        "exposure_curve": exposure_curve,
        "trade_round_trips": trade_round_trips,
    }
    return report


def build_summary(report: dict[str, Any]) -> dict[str, Any]:
    meta = dict(report.get("run_metadata") or {})
    perf = dict(report.get("performance") or {})
    trades = dict(report.get("trades") or {})
    quality = dict(report.get("quality") or {})
    return {
        "report_schema_version": REPORT_SCHEMA_VERSION,
        "run_id": meta.get("run_id"),
        "engine_mode": meta.get("engine_mode"),
        "strategy_name": meta.get("strategy_name"),
        "symbol": meta.get("symbol"),
        "interval": meta.get("interval"),
        "start_ts_ms": meta.get("start_ts_ms"),
        "end_ts_ms": meta.get("end_ts_ms"),
        "total_steps": meta.get("total_steps"),
        "total_fills": trades.get("total_fills"),
        "config_hash": meta.get("config_hash"),
        "git_sha": meta.get("git_sha"),
        "quality_verdict": quality.get("verdict"),
        "warning_count": quality.get("warning_counts"),
        "performance_summary": {
            "total_return": perf.get("total_return"),
            "max_drawdown": perf.get("max_drawdown"),
            "sharpe": perf.get("sharpe_ratio"),
            "total_trades": trades.get("total_trades"),
        },
        "report_generated_at": report.get("report_generated_at"),
    }


def _update_run_index(*, runs_root: Path) -> None:
    rows: list[dict[str, Any]] = []
    for summary_path in runs_root.glob("*/report/summary.json"):
        try:
            obj = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        run_id = str(obj.get("run_id") or "")
        if not run_id:
            continue
        perf = dict(obj.get("performance_summary") or {})
        rows.append(
            {
                "run_id": run_id,
                "strategy_name": obj.get("strategy_name"),
                "mode": str(obj.get("engine_mode") or "").lower(),
                "interval": obj.get("interval"),
                "start_ts_ms": obj.get("start_ts_ms"),
                "end_ts_ms": obj.get("end_ts_ms"),
                "total_return_pct": _safe_float(perf.get("total_return")) * 100.0,
                "max_drawdown_pct": _safe_float(perf.get("max_drawdown")) * 100.0,
                "sharpe_ratio": _safe_float(perf.get("sharpe")),
                "total_trades": int(perf.get("total_trades") or 0),
                "quality_verdict": obj.get("quality_verdict"),
                "view_paths": {
                    "backtest_card": f"{run_id}/views/backtest_card.json",
                    "performance_summary": f"{run_id}/views/performance_summary.json",
                    "drawdown_chart": f"{run_id}/views/drawdown_chart.json",
                },
            }
        )
    rows.sort(key=lambda r: int(r.get("start_ts_ms") or 0), reverse=True)
    payload = {
        "schema": RUN_INDEX_SCHEMA,
        "generated_at": _now_iso(),
        "runs": rows,
    }
    (runs_root / "_index.json").write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")


def generate_backtest_artifacts(*, run_dir: Path, run_id: str) -> dict[str, Any]:
    trace_path = _find_trace_path(run_dir)
    report = build_report_from_trace(trace_path=trace_path, run_id=run_id)
    summary = build_summary(report)

    report_dir = run_dir / "report"
    views_dir = run_dir / "views"
    report_dir.mkdir(parents=True, exist_ok=True)
    views_dir.mkdir(parents=True, exist_ok=True)

    (report_dir / "report.json").write_text(json.dumps(report, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    (report_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=True, sort_keys=True), encoding="utf-8")

    backtest_card = build_backtest_card(report)
    equity_chart = build_equity_chart(report)
    drawdown_chart = build_drawdown_chart(report)
    performance_summary = build_performance_summary(report)
    warning_list = build_warning_list(report)
    pnl_period_chart = build_pnl_period_chart(report)
    exposure_chart = build_exposure_chart(report)
    trade_pnl_histogram = build_trade_pnl_histogram(report)
    trade_list = build_trade_list(report)
    quality_badge = build_quality_badge(report)
    (views_dir / "backtest_card.json").write_text(json.dumps(backtest_card, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    (views_dir / "equity_chart.json").write_text(json.dumps(equity_chart, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    (views_dir / "drawdown_chart.json").write_text(json.dumps(drawdown_chart, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    (views_dir / "performance_summary.json").write_text(json.dumps(performance_summary, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    (views_dir / "warning_list.json").write_text(json.dumps(warning_list, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    (views_dir / "pnl_period_chart.json").write_text(json.dumps(pnl_period_chart, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    (views_dir / "exposure_chart.json").write_text(json.dumps(exposure_chart, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    (views_dir / "trade_pnl_histogram.json").write_text(json.dumps(trade_pnl_histogram, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    (views_dir / "trade_list.json").write_text(json.dumps(trade_list, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    (views_dir / "quality_badge.json").write_text(json.dumps(quality_badge, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    try:
        _update_run_index(runs_root=run_dir.parent)
    except Exception:
        # Keep per-run artifacts deterministic even if index refresh fails.
        pass
    return {
        "report_path": str(report_dir / "report.json"),
        "summary_path": str(report_dir / "summary.json"),
        "views_path": str(views_dir),
        "index_path": str(run_dir.parent / "_index.json"),
    }
