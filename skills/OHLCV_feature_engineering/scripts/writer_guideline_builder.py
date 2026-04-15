from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import load_json, read_template, write_text


def _bindable_params(spec: dict[str, Any]) -> dict[str, Any]:
    params = spec["formula"]["parameters"]
    bindable: dict[str, Any] = {}
    for key in ("window_bars", "slow_window_bars", "fast_window_bars", "normalization_window_bars"):
        value = params.get(key)
        if value not in (None, {}, []):
            bindable[key] = value
    for key, value in (params.get("other") or {}).items():
        if value not in (None, {}, []):
            bindable[key] = value
    return bindable


def _writer_action(verdict: str) -> str:
    if verdict == "keep":
        return "Implement if the current feature batch needs this mechanism coverage."
    if verdict == "defer":
        return "Do not implement in the first pass unless another feature in the same mechanism group is unavailable."
    return "Do not implement."


def run(features_path: Path, quality_path: Path, output_path: Path) -> str:
    features_payload = load_json(features_path)
    quality_payload = load_json(quality_path)
    quality_map = {row["feature_id"]: row for row in quality_payload["quality_report"]}

    blocks: list[str] = []
    for spec in features_payload["feature_specifications"]:
        quality = quality_map.get(spec["feature_id"])
        if quality is None:
            continue
        bindable = _bindable_params(spec)
        strengths = quality.get("strengths", [])
        weaknesses = quality.get("weaknesses", [])
        failure_modes = quality.get("failure_modes", [])
        blocks.append(f"## {spec['feature_id']} - {spec['feature_name_template']}")
        blocks.append("")
        blocks.append(f"- Verdict: `{quality['verdict']}`")
        blocks.append(f"- Writer action: {_writer_action(quality['verdict'])}")
        blocks.append(f"- Role: `{quality.get('feature_role', spec.get('feature_role'))}`")
        blocks.append(f"- Family: `{spec['family']}`")
        if spec.get("secondary_family"):
            blocks.append(f"- Secondary family: `{spec['secondary_family']}`")
        blocks.append(f"- What it measures: {spec['description']}")
        blocks.append(f"- Formula: `{spec['formula']['expression']}`")
        blocks.append(f"- Inputs: `{spec['formula']['inputs']}`")
        blocks.append(f"- Bindable strategy params: `{bindable}`")
        blocks.append(f"- Required window: `{spec['warmup']['required_window_by_domain']}`")
        blocks.append(f"- Warmup minimum bars: `{spec['warmup']['minimum_history_bars']}`")
        blocks.append(f"- Timing rule: `{spec['timing_semantics']['decision_timestamp_rule']}`")
        blocks.append(f"- Bar state: `{spec['timing_semantics']['bar_state']}`")
        if spec["timing_semantics"].get("decision_lag_bars") is not None:
            blocks.append(f"- Decision lag bars: `{spec['timing_semantics']['decision_lag_bars']}`")
        blocks.append("- Implementation guidance:")
        blocks.append("  - Keep `update()` incremental and bounded; do not rescan full history inside each step.")
        blocks.append("  - Return `None` until warmup is satisfied.")
        blocks.append(f"  - Zero division policy: {spec['missing_data_policy']['zero_division']}")
        blocks.append(f"  - Insufficient history policy: {spec['missing_data_policy']['insufficient_history']}")
        blocks.append("- Research facts:")
        for item in strengths[:3]:
            blocks.append(f"  - Strength: {item}")
        for item in weaknesses[:2]:
            blocks.append(f"  - Weakness: {item}")
        for item in failure_modes[:3]:
            blocks.append(f"  - Failure mode: {item}")
        blocks.append("")

    template = read_template("writer_guidelines_template.md")
    rendered = template.format(
        direction_id=features_payload["parsed_directions"]["direction_id"],
        observation_interval=features_payload["parsed_directions"]["observation_interval"],
        feature_blocks="\n".join(blocks).strip(),
    )
    write_text(output_path, rendered)
    return rendered


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", required=True)
    parser.add_argument("--quality", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    run(Path(args.features), Path(args.quality), Path(args.output))


if __name__ == "__main__":
    main()
