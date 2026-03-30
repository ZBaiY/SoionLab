from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import dump_json, family_from_type, load_json, yaml_dump, write_text


def run(input_path: Path, output_path: Path, yaml_output: Path | None = None) -> dict[str, Any]:
    payload = load_json(input_path)
    classifications: list[dict[str, Any]] = []
    for spec in payload["feature_specifications"]:
        family, secondary = family_from_type(spec["feature_type_label"], spec["formula"]["expression"], spec["formula"]["inputs"])
        spec["family"] = family
        spec["secondary_family"] = spec.get("secondary_family") or secondary
        classifications.append({
            "feature_id": spec["feature_id"],
            "family": spec["family"],
            "secondary_family": spec["secondary_family"],
            "failure_modes": {
                "price_path": ["raw price level dependence", "future path summary", "overlap with simple returns"],
                "volatility": ["forward-looking realized vol", "short-window instability", "direction/vol conflation"],
                "volume_liquidity": ["nonstationary raw volume", "missing normalization", "session mix effects"],
                "order_flow_proxy": ["proxy overstated as true flow", "same-family duplication", "precision overclaim"],
                "inventory_state": ["full-sample anchor seeding", "future volume refresh in anchor updates", "state collapsing into a disguised price-path transform"],
                "trade_activity": ["optional aux fields missing", "nonportable venue dependence", "raw trade-tape assumptions"],
                "time_structure": ["calendar portability issues", "implicit future close awareness", "spurious bucket overfit"],
                "cross_interaction": ["feature explosion", "high collinearity", "reference alignment ambiguity"],
            }[spec["family"]],
        })
    result = {
        "parsed_directions": payload["parsed_directions"],
        "hypotheses": payload["hypotheses"],
        "feature_specifications": payload["feature_specifications"],
        "timing_audit": payload.get("timing_audit", []),
        "family_classification": classifications,
    }
    dump_json(output_path, result)
    if yaml_output is not None:
        final_yaml = {
            "parsed_directions": result["parsed_directions"],
            "feature_specifications": result["feature_specifications"],
        }
        write_text(yaml_output, yaml_dump(final_yaml) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--yaml-output")
    args = parser.parse_args()
    run(Path(args.input), Path(args.output), Path(args.yaml_output) if args.yaml_output else None)


if __name__ == "__main__":
    main()
