from __future__ import annotations

import argparse
import json
from pathlib import Path

from common import iter_trace_steps, parse_feature_key_map, safe_float, write_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare two runs step-by-step on trace decisions.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--lhs-run-id", required=True)
    parser.add_argument("--rhs-run-id", required=True)
    parser.add_argument(
        "--feature-keys",
        default="",
        help="comma-separated feature aliases to include, e.g. trend=ADX...,osc=RSI...,aux=MACD...",
    )
    parser.add_argument("--out-csv", required=True)
    parser.add_argument("--out-json", default=None)
    args = parser.parse_args()

    key_map = parse_feature_key_map(args.feature_keys)
    lhs = {int(step.get("ts_ms") or 0): step for step in iter_trace_steps(Path(args.repo_root), args.lhs_run_id)}
    rhs = {int(step.get("ts_ms") or 0): step for step in iter_trace_steps(Path(args.repo_root), args.rhs_run_id)}
    rows: list[dict[str, object]] = []
    for ts_ms in sorted(set(lhs) & set(rhs)):
        lhs_step = lhs[ts_ms]
        rhs_step = rhs[ts_ms]
        lhs_decision = safe_float(lhs_step.get("decision_score"))
        rhs_decision = safe_float(rhs_step.get("decision_score"))
        if lhs_decision == rhs_decision:
            continue
        lhs_features = dict(lhs_step.get("features") or {})
        rhs_features = dict(rhs_step.get("features") or {})
        row: dict[str, object] = {
            "ts_ms": ts_ms,
            "lhs_run_id": args.lhs_run_id,
            "rhs_run_id": args.rhs_run_id,
            "lhs_decision": lhs_decision,
            "rhs_decision": rhs_decision,
        }
        for alias, key in key_map.items():
            row[f"lhs_{alias}"] = safe_float(lhs_features.get(key))
            row[f"rhs_{alias}"] = safe_float(rhs_features.get(key))
        rows.append(row)

    write_csv(Path(args.out_csv), rows)
    if args.out_json:
        Path(args.out_json).write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
