from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from math import ceil
from typing import Any

from analyze.views.schemas import (
    BACKTEST_CARD_SCHEMA,
    DRAWDOWN_CHART_SCHEMA,
    EQUITY_CHART_SCHEMA,
    EXPOSURE_CHART_SCHEMA,
    PERFORMANCE_SUMMARY_SCHEMA,
    PNL_PERIOD_CHART_SCHEMA,
    QUALITY_BADGE_SCHEMA,
    TRADE_LIST_SCHEMA,
    TRADE_PNL_HISTOGRAM_SCHEMA,
    WARNING_LIST_SCHEMA,
)

_MAX_EQUITY_POINTS = 1000
_SEVERITY_RANK = {"critical": 0, "warning": 1, "info": 2}
_CATEGORY_RANK = {
    "lookahead": 0,
    "accounting": 1,
    "data_integrity": 2,
    "completeness": 3,
    "execution_realism": 4,
}
_VERDICT_META = {
    "CLEAN": ("Clean", "No warnings detected"),
    "QUALIFIED": ("Qualified", "Minor warnings present; results usable with caveats"),
    "DEGRADED": ("Degraded", "Significant warnings present; interpret results with caution"),
    "INVALID": ("Invalid", "Critical issues detected; run should not be trusted"),
}


def _iso_utc(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _pct(x: Any) -> float:
    return float(x or 0.0) * 100.0


def _downsample_series(
    series: list[dict[str, Any]],
    *,
    max_points: int = _MAX_EQUITY_POINTS,
    key: str = "t",
) -> list[dict[str, Any]]:
    n = len(series)
    if n <= max_points:
        return series
    step = int(ceil(float(n) / float(max_points)))
    out: list[dict[str, float]] = []
    for i in range(0, n, step):
        out.append(series[i])
    if out[-1].get(key) != series[-1].get(key):
        out.append(series[-1])
    return out


def build_backtest_card(report: dict[str, Any]) -> dict[str, Any]:
    meta = dict(report.get("run_metadata") or {})
    perf = dict(report.get("performance") or {})
    trades = dict(report.get("trades") or {})
    quality = dict(report.get("quality") or {})
    counts = dict(quality.get("warning_counts") or {})
    start_ms = meta.get("start_ts_ms")
    end_ms = meta.get("end_ts_ms")
    duration_days = 0.0
    if isinstance(start_ms, int) and isinstance(end_ms, int) and end_ms >= start_ms:
        duration_days = float(end_ms - start_ms) / 86_400_000.0
    return {
        "schema": BACKTEST_CARD_SCHEMA,
        "run_id": str(meta.get("run_id") or ""),
        "strategy": str(meta.get("strategy_name") or ""),
        "symbol": str(meta.get("symbol") or ""),
        "interval": str(meta.get("interval") or ""),
        "mode": "backtest",
        "time_range": {
            "start": _iso_utc(start_ms),
            "end": _iso_utc(end_ms),
            "start_ms": start_ms,
            "end_ms": end_ms,
            "duration_days": duration_days,
        },
        "performance": {
            "total_return_pct": _pct(perf.get("total_return")),
            "annualized_return_pct": _pct(perf.get("annualized_return")),
            "max_drawdown_pct": _pct(perf.get("max_drawdown")),
            "sharpe_ratio": float(perf.get("sharpe_ratio") or 0.0),
            "sortino_ratio": float(perf.get("sortino_ratio") or 0.0),
            "calmar_ratio": float(perf.get("calmar_ratio") or 0.0),
        },
        "trades": {
            "total_trades": int(trades.get("total_trades") or 0),
            "win_rate_pct": _pct(trades.get("win_rate")),
            "profit_factor": float(trades.get("profit_factor") or 0.0),
            "avg_holding_steps": int(trades.get("avg_holding_period_steps") or 0),
        },
        "quality": {
            "verdict": str(quality.get("verdict") or "CLEAN"),
            "critical_warnings": int(counts.get("critical") or 0),
            "warnings": int(counts.get("warning") or 0),
            "info": int(counts.get("info") or 0),
        },
        "meta": {
            "config_hash": meta.get("config_hash"),
            "git_sha": meta.get("git_sha"),
            "report_generated_at": report.get("report_generated_at"),
        },
    }


def build_equity_chart(report: dict[str, Any]) -> dict[str, Any]:
    meta = dict(report.get("run_metadata") or {})
    perf = dict(report.get("performance") or {})
    curve = list(report.get("equity_curve") or [])
    peak = None
    series: list[dict[str, Any]] = []
    min_eq = None
    max_eq = None
    for item in curve:
        if not (isinstance(item, (list, tuple)) and len(item) >= 2):
            continue
        ts_ms = int(item[0])
        eq = float(item[1])
        peak = eq if peak is None else max(peak, eq)
        dd = 0.0 if peak in (None, 0.0) else ((eq - peak) / peak) * 100.0
        series.append(
            {
                "ts": _iso_utc(ts_ms),
                "ts_ms": ts_ms,
                "total_equity": eq,
                "drawdown_pct": dd,
            }
        )
        min_eq = eq if min_eq is None else min(min_eq, eq)
        max_eq = eq if max_eq is None else max(max_eq, eq)
    series = _downsample_series(series, key="ts_ms")
    return {
        "schema": EQUITY_CHART_SCHEMA,
        "run_id": str(meta.get("run_id") or ""),
        "interval": str(meta.get("interval") or ""),
        "series": series,
        "summary": {
            "min_equity": float(min_eq or 0.0),
            "max_equity": float(max_eq or 0.0),
            "final_equity": float(perf.get("final_equity") or 0.0),
            "peak_drawdown_pct": _pct(perf.get("max_drawdown")),
            "total_points": len(curve),
        },
    }


def build_drawdown_chart(report: dict[str, Any]) -> dict[str, Any]:
    meta = dict(report.get("run_metadata") or {})
    curve = list(report.get("equity_curve") or [])
    peak = None
    series: list[dict[str, Any]] = []
    min_dd = 0.0
    for item in curve:
        if not (isinstance(item, (list, tuple)) and len(item) >= 2):
            continue
        ts_ms = int(item[0])
        eq = float(item[1])
        peak = eq if peak is None else max(peak, eq)
        dd_pct = 0.0 if peak in (None, 0.0) else ((eq - peak) / peak) * 100.0
        min_dd = min(min_dd, dd_pct)
        series.append(
            {
                "ts": _iso_utc(ts_ms),
                "ts_ms": ts_ms,
                "drawdown_pct": dd_pct,
            }
        )
    series = _downsample_series(series, key="ts_ms")
    return {
        "schema": DRAWDOWN_CHART_SCHEMA,
        "run_id": str(meta.get("run_id") or ""),
        "interval": str(meta.get("interval") or ""),
        "series": series,
        "summary": {
            "max_drawdown_pct": min_dd,
            "total_points": len(curve),
        },
    }


def build_performance_summary(report: dict[str, Any]) -> dict[str, Any]:
    meta = dict(report.get("run_metadata") or {})
    perf = dict(report.get("performance") or {})
    trades = dict(report.get("trades") or {})
    quality = dict(report.get("quality") or {})
    return {
        "schema": PERFORMANCE_SUMMARY_SCHEMA,
        "run_id": str(meta.get("run_id") or ""),
        "strategy": str(meta.get("strategy_name") or ""),
        "symbol": str(meta.get("symbol") or ""),
        "interval": str(meta.get("interval") or ""),
        "time_range": {
            "start": _iso_utc(meta.get("start_ts_ms")),
            "start_ms": meta.get("start_ts_ms"),
            "end": _iso_utc(meta.get("end_ts_ms")),
            "end_ms": meta.get("end_ts_ms"),
        },
        "metrics": {
            "total_return_pct": _pct(perf.get("total_return")),
            "annualized_return_pct": _pct(perf.get("annualized_return")),
            "max_drawdown_pct": _pct(perf.get("max_drawdown")),
            "sharpe_ratio": float(perf.get("sharpe_ratio") or 0.0),
            "sortino_ratio": float(perf.get("sortino_ratio") or 0.0),
            "calmar_ratio": float(perf.get("calmar_ratio") or 0.0),
            "trade_count": int(trades.get("total_trades") or 0),
            "win_rate_pct": _pct(trades.get("win_rate")),
            "profit_factor": float(trades.get("profit_factor") or 0.0),
        },
        "quality": {
            "verdict": str(quality.get("verdict") or "CLEAN"),
            "critical": int((quality.get("warning_counts") or {}).get("critical") or 0),
            "warning": int((quality.get("warning_counts") or {}).get("warning") or 0),
            "info": int((quality.get("warning_counts") or {}).get("info") or 0),
        },
    }


def build_warning_list(report: dict[str, Any]) -> dict[str, Any]:
    meta = dict(report.get("run_metadata") or {})
    quality = dict(report.get("quality") or {})
    warnings = list(quality.get("warnings") or [])
    warnings.sort(
        key=lambda w: (
            _SEVERITY_RANK.get(str(w.get("severity") or "info"), 99),
            _CATEGORY_RANK.get(str(w.get("category") or ""), 99),
            str(w.get("id") or ""),
        )
    )
    items = []
    for w in warnings:
        ar = dict(w.get("affected_range") or {})
        items.append(
            {
                "id": str(w.get("id") or ""),
                "category": str(w.get("category") or ""),
                "severity": str(w.get("severity") or ""),
                "title": str(w.get("title") or ""),
                "summary": str(w.get("description") or ""),
                "start_ts": _iso_utc(ar.get("start_ts_ms")),
                "start_ts_ms": ar.get("start_ts_ms"),
                "end_ts": _iso_utc(ar.get("end_ts_ms")),
                "end_ts_ms": ar.get("end_ts_ms"),
            }
        )
    return {
        "schema": WARNING_LIST_SCHEMA,
        "run_id": str(meta.get("run_id") or ""),
        "verdict": str(quality.get("verdict") or "CLEAN"),
        "items": items,
    }


def build_pnl_period_chart(report: dict[str, Any]) -> dict[str, Any]:
    meta = dict(report.get("run_metadata") or {})
    curve = list(report.get("equity_curve") or [])
    if not curve:
        return {
            "schema": PNL_PERIOD_CHART_SCHEMA,
            "run_id": str(meta.get("run_id") or ""),
            "period": "day",
            "series": [],
        }
    by_day: dict[str, dict[str, float | int]] = defaultdict(dict)
    prev_equity = float(curve[0][1])
    for idx, item in enumerate(curve):
        ts_ms = int(item[0])
        eq = float(item[1])
        day_key = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        bucket = by_day[day_key]
        if "start_ms" not in bucket:
            bucket["start_ms"] = ts_ms
            bucket["start_equity"] = prev_equity if idx > 0 else eq
            bucket["pnl"] = 0.0
        bucket["end_ms"] = ts_ms
        bucket["end_equity"] = eq
        prev_equity = eq
    series = []
    for day in sorted(by_day.keys()):
        b = by_day[day]
        start_eq = float(b.get("start_equity") or 0.0)
        end_eq = float(b.get("end_equity") or start_eq)
        pnl = end_eq - start_eq
        ret_pct = (pnl / start_eq) * 100.0 if start_eq != 0 else 0.0
        series.append(
            {
                "period": day,
                "start_ts": _iso_utc(int(b.get("start_ms") or 0)),
                "start_ts_ms": int(b.get("start_ms") or 0),
                "end_ts": _iso_utc(int(b.get("end_ms") or 0)),
                "end_ts_ms": int(b.get("end_ms") or 0),
                "pnl": pnl,
                "return_pct": ret_pct,
            }
        )
    return {
        "schema": PNL_PERIOD_CHART_SCHEMA,
        "run_id": str(meta.get("run_id") or ""),
        "period": "day",
        "series": series,
    }


def build_exposure_chart(report: dict[str, Any]) -> dict[str, Any]:
    meta = dict(report.get("run_metadata") or {})
    exposures = list(report.get("exposure_curve") or [])
    series = []
    for item in exposures:
        if not (isinstance(item, (list, tuple)) and len(item) >= 2):
            continue
        ts_ms = int(item[0])
        exposure = float(item[1])
        series.append(
            {
                "ts": _iso_utc(ts_ms),
                "ts_ms": ts_ms,
                "exposure": exposure,
            }
        )
    series = _downsample_series(series, key="ts_ms")
    return {
        "schema": EXPOSURE_CHART_SCHEMA,
        "run_id": str(meta.get("run_id") or ""),
        "interval": str(meta.get("interval") or ""),
        "series": series,
    }


def build_trade_pnl_histogram(report: dict[str, Any], *, bins: int = 10) -> dict[str, Any]:
    meta = dict(report.get("run_metadata") or {})
    trades = list(report.get("trade_round_trips") or [])
    vals = [float(t.get("net_pnl") or 0.0) for t in trades]
    if not vals:
        return {
            "schema": TRADE_PNL_HISTOGRAM_SCHEMA,
            "run_id": str(meta.get("run_id") or ""),
            "buckets": [],
        }
    min_v = min(vals)
    max_v = max(vals)
    if min_v == max_v:
        return {
            "schema": TRADE_PNL_HISTOGRAM_SCHEMA,
            "run_id": str(meta.get("run_id") or ""),
            "buckets": [
                {
                    "start": min_v,
                    "end": max_v,
                    "count": len(vals),
                }
            ],
        }
    width = (max_v - min_v) / float(max(1, bins))
    counts = [0 for _ in range(max(1, bins))]
    for v in vals:
        idx = int((v - min_v) / width)
        if idx >= len(counts):
            idx = len(counts) - 1
        counts[idx] += 1
    buckets = []
    for i, c in enumerate(counts):
        start = min_v + float(i) * width
        end = start + width
        buckets.append({"start": start, "end": end, "count": int(c)})
    return {
        "schema": TRADE_PNL_HISTOGRAM_SCHEMA,
        "run_id": str(meta.get("run_id") or ""),
        "buckets": buckets,
    }


def build_trade_list(report: dict[str, Any], *, offset: int = 0, limit: int = 50) -> dict[str, Any]:
    meta = dict(report.get("run_metadata") or {})
    all_trades = list(report.get("trade_round_trips") or [])
    window = all_trades[offset : offset + limit]
    return {
        "schema": TRADE_LIST_SCHEMA,
        "run_id": str(meta.get("run_id") or ""),
        "trades": window,
        "pagination": {
            "total": len(all_trades),
            "offset": int(offset),
            "limit": int(limit),
        },
    }


def build_quality_badge(report: dict[str, Any]) -> dict[str, Any]:
    meta = dict(report.get("run_metadata") or {})
    quality = dict(report.get("quality") or {})
    warnings = list(quality.get("warnings") or [])
    warnings.sort(
        key=lambda w: (
            _SEVERITY_RANK.get(str(w.get("severity") or "info"), 99),
            _CATEGORY_RANK.get(str(w.get("category") or ""), 99),
            str(w.get("id") or ""),
        )
    )
    top = []
    for item in warnings[:5]:
        top.append(
            {
                "category": str(item.get("category") or ""),
                "severity": str(item.get("severity") or ""),
                "title": str(item.get("title") or ""),
                "summary": str(item.get("description") or ""),
            }
        )
    verdict = str(quality.get("verdict") or "CLEAN")
    label, desc = _VERDICT_META.get(verdict, ("Unknown", "Unknown quality state"))
    return {
        "schema": QUALITY_BADGE_SCHEMA,
        "run_id": str(meta.get("run_id") or ""),
        "verdict": verdict,
        "verdict_label": label,
        "verdict_description": desc,
        "counts": {
            "critical": int((quality.get("warning_counts") or {}).get("critical") or 0),
            "warning": int((quality.get("warning_counts") or {}).get("warning") or 0),
            "info": int((quality.get("warning_counts") or {}).get("info") or 0),
        },
        "top_warnings": top,
    }
