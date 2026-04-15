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


def run(features_path: Path, quality_path: Path, output_path: Path) -> str:
    features_payload = load_json(features_path)
    quality_payload = load_json(quality_path)
    quality_map = {row["feature_id"]: row for row in quality_payload["quality_report"]}

    lines: list[str] = []
    for spec in features_payload["feature_specifications"]:
        quality = quality_map[spec["feature_id"]]
        if quality["verdict"] == "reject":
            continue
        lines.append(f"## {spec['feature_id']} - {spec['feature_name_template']}")
        lines.append("")
        lines.append(f"- Verdict: `{quality['verdict']}`")
        lines.append(f"- Feature type: `{spec['feature_type_label']}`")
        lines.append(f"- Family: `{spec['family']}`")
        lines.append(f"- Formula: `{spec['formula']['expression']}`")
        lines.append(f"- Required inputs: `{spec['data_requirements']['required_fields']}`")
        lines.append(f"- Constructor params: `{spec['formula']['parameters']}`")
        lines.append(f"- Bindable strategy params: `{_bindable_params(spec)}`")
        lines.append(f"- Required window: `{spec['warmup']['required_window_by_domain']}`")
        lines.append("- Initialize requirements:")
        lines.append(f"  - seed only from trailing visible bars covering at least {spec['warmup']['minimum_history_bars']} bars")
        lines.append("  - preserve deterministic state on repeated warmup/replay")
        lines.append("- Update requirements:")
        lines.append("  - update on each engine step using only bars with `data_ts <= visible_end_ts(step_ts)`")
        lines.append("  - do not depend on partial bars or ingestion cadence")
        lines.append("- Output contract:")
        lines.append("  - return a scalar numeric feature value or `None` until warmup is satisfied")
        lines.append("- Edge cases:")
        lines.append(f"  - {spec['missing_data_policy']['insufficient_history']}")
        lines.append(f"  - {spec['missing_data_policy']['zero_division']}")
        lines.append("- Tests required:")
        lines.append("  - warmup length and `required_window()` correctness")
        lines.append("  - closed-bar visibility at decision time")
        lines.append("  - deterministic replay")
        lines.append("  - constant-price and zero-range behavior")
        lines.append("")

    template = read_template("implementation_handoff_template.md")
    rendered = template.format(
        direction_id=features_payload["parsed_directions"]["direction_id"],
        observation_interval=features_payload["parsed_directions"]["observation_interval"],
        task_blocks="\n".join(lines).strip(),
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
