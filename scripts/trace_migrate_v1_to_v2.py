#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from quant_engine.utils.logger import TRACE_HEADER_EVENT, TRACE_SCHEMA_VERSION

FORBIDDEN_KEYS = {"module", "msg", "category", "logger"}


def _iso_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _infer_execution_constraints(context: dict[str, Any]) -> dict[str, Any]:
    portfolio = context.get("portfolio")
    step_size = None
    min_notional = None
    if isinstance(portfolio, dict):
        step_size = portfolio.get("step_size") or portfolio.get("qty_step")
        min_notional = portfolio.get("min_notional")
    fractional = False
    if step_size is not None:
        try:
            fractional = float(step_size) < 1.0
        except Exception:
            fractional = False
    rounding_policy = "step_floor" if fractional else "integer_floor"
    return {
        "fractional": bool(fractional),
        "min_lot": float(step_size) if step_size is not None else None,
        "min_notional": float(min_notional) if min_notional is not None else None,
        "rounding_policy": rounding_policy if step_size is not None else "unknown",
    }


def _infer_header(first: dict[str, Any]) -> dict[str, Any]:
    context = first.get("context")
    if not isinstance(context, dict):
        context = {}
    run_id = first.get("run_id") or context.get("run_id") or "unknown"
    mode = first.get("mode") or context.get("mode")
    engine_mode = str(mode).upper() if mode else "unknown"
    strategy_name = first.get("strategy") or context.get("strategy") or "unknown"
    interval = first.get("interval") or context.get("interval") or "unknown"
    start_ts_ms = first.get("ts_ms") or first.get("timestamp")
    start_ts = first.get("ts") or _iso_from_ms(start_ts_ms)
    return {
        "ts_ms": start_ts_ms,
        "event": TRACE_HEADER_EVENT,
        "schema_version": TRACE_SCHEMA_VERSION,
        "run_id": run_id,
        "engine_mode": engine_mode,
        "engine_git_sha": "unknown",
        "config_hash": "unknown",
        "strategy_name": strategy_name,
        "interval": interval,
        "execution_constraints": _infer_execution_constraints(context),
        "start_ts_ms": start_ts_ms,
        "start_ts": start_ts,
    }


def _migrate_record(rec: dict[str, Any]) -> dict[str, Any]:
    context = rec.get("context")
    if not isinstance(context, dict):
        context = {}

    event = rec.get("event") or rec.get("msg") or "unknown"
    out: dict[str, Any] = {
        "ts_ms": rec.get("ts_ms") or rec.get("timestamp"),
        "event": event,
    }
    run_id = rec.get("run_id") or context.get("run_id")
    if run_id is not None:
        out["run_id"] = run_id
    if "level" in rec:
        out["level"] = rec["level"]

    for key, value in rec.items():
        if key in FORBIDDEN_KEYS or key in {"ts", "ts_ms", "event", "run_id", "level", "context"}:
            continue
        out[key] = value

    for key, value in context.items():
        if key in {"run_id", "mode", "category"}:
            continue
        if key not in out:
            out[key] = value
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate trace.jsonl v1 -> v2")
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    lines = args.input.read_text(encoding="utf-8").splitlines()
    if not lines:
        return 0

    first = json.loads(lines[0])
    if first.get("event") == TRACE_HEADER_EVENT and first.get("schema_version") == TRACE_SCHEMA_VERSION:
        args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return 0

    header = _infer_header(first)
    out_lines = [json.dumps(header, ensure_ascii=False)]
    for line in lines:
        rec = json.loads(line)
        out_lines.append(json.dumps(_migrate_record(rec), ensure_ascii=False))

    args.output.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
