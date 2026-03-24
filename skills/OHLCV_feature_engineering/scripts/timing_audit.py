from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import allowed_fields, dump_json, load_json


LEAKY_TOKENS = [
    "future",
    "forward",
    "next_bar",
    "lead(",
    "t+1",
    "t_plus_1",
    "max_future",
    "min_future",
    "realized_forward",
]


def audit_feature(spec: dict[str, Any], parsed: dict[str, Any]) -> tuple[str, list[str]]:
    notes: list[str] = []
    verdict = "pass"
    expression = spec["formula"]["expression"].lower()
    inputs = [str(item).lower() for item in spec["formula"]["inputs"]]
    params = spec["formula"]["parameters"]
    available = allowed_fields(parsed.get("available_fields"))

    for token in LEAKY_TOKENS:
        if token in expression or any(token in item for item in inputs):
            return "reject", [f"Leaky token detected: {token}"]

    if spec["timing_semantics"]["decision_timestamp_rule"] != "use data_ts <= visible_end_ts(step_ts)":
        return "reject", ["Decision timestamp rule does not match repo closed-bar contract."]

    if spec["timing_semantics"]["bar_state"] != "closed_only":
        return "reject", ["Bar state must be closed_only."]

    for key in ("window_bars", "slow_window_bars", "fast_window_bars", "normalization_window_bars"):
        value = params.get(key)
        if value is not None and (not isinstance(value, int) or value <= 0):
            return "reject", [f"Invalid trailing window parameter: {key}={value!r}"]

    missing_required = [field for field in spec["data_requirements"]["required_fields"] if field not in available]
    if missing_required:
        verdict = "clarify"
        notes.append(f"Required fields not guaranteed by declared available_fields: {missing_required}")

    if spec["data_requirements"]["reference_symbol_required"] and not parsed["input_domain_scope"]["reference_symbols"]:
        verdict = "clarify"
        notes.append("Reference symbol required but not supplied.")

    if "step_ts" in inputs and spec["family"] != "time_structure":
        verdict = "clarify"
        notes.append("Direct step_ts input is only expected for time_structure features.")

    if "step_ts" in inputs and spec["family"] == "time_structure":
        notes.append("Time-structure feature is causal when derived directly from decision-time step_ts.")

    if spec["family"] == "trade_activity" and not any(
        field in available for field in ("number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume")
    ):
        verdict = "clarify"
        notes.append("Trade activity candidate depends on optional OHLCV aux fields that are not guaranteed.")

    if not notes and verdict == "pass":
        notes.append("Causal under closed-bar OHLCV semantics.")
    return verdict, notes


def run(input_path: Path, output_path: Path) -> dict[str, Any]:
    payload = load_json(input_path)
    parsed = payload["parsed_directions"]
    timing_audit: list[dict[str, Any]] = []
    for spec in payload["feature_specifications"]:
        verdict, notes = audit_feature(spec, parsed)
        spec["audit_status"] = verdict
        spec["audit_notes"] = notes
        timing_audit.append({
            "feature_id": spec["feature_id"],
            "verdict": verdict,
            "notes": notes,
        })
    result = {
        "parsed_directions": parsed,
        "hypotheses": payload["hypotheses"],
        "feature_specifications": payload["feature_specifications"],
        "timing_audit": timing_audit,
    }
    dump_json(output_path, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    run(Path(args.input), Path(args.output))


if __name__ == "__main__":
    main()
