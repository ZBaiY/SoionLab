#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml


REQUIRED_FILES = [
    "feature_specs.yaml",
    "implementation_handoff.md",
    "writer_guidelines.md",
]

OPTIONAL_FILES = [
    "quality_report.md",
    "redundancy_analysis.json",
    "library_record.json",
]


def repo_root_from_here() -> Path:
    return Path(__file__).resolve().parents[3]


def resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else (repo_root_from_here() / path)


def load_yaml(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate an OHLCV implementation run bundle read-set.")
    parser.add_argument("--run-bundle", required=True, help="Path to one OHLCV research run bundle.")
    parser.add_argument("--json", action="store_true", help="Emit JSON summary instead of text.")
    args = parser.parse_args()

    run_bundle = resolve_path(args.run_bundle)
    missing_required: list[str] = []
    malformed: list[str] = []
    found_optional: list[str] = []

    for rel in REQUIRED_FILES:
        path = run_bundle / rel
        if not path.exists():
            missing_required.append(rel)

    if (run_bundle / "feature_specs.yaml").exists():
        try:
            load_yaml(run_bundle / "feature_specs.yaml")
        except Exception as exc:
            malformed.append(f"feature_specs.yaml: {exc}")

    if (run_bundle / "redundancy_analysis.json").exists():
        try:
            load_json(run_bundle / "redundancy_analysis.json")
        except Exception as exc:
            malformed.append(f"redundancy_analysis.json: {exc}")
        else:
            found_optional.append("redundancy_analysis.json")

    if (run_bundle / "library_record.json").exists():
        try:
            load_json(run_bundle / "library_record.json")
        except Exception as exc:
            malformed.append(f"library_record.json: {exc}")
        else:
            found_optional.append("library_record.json")

    if (run_bundle / "quality_report.md").exists():
        found_optional.append("quality_report.md")

    summary = {
        "run_bundle": str(run_bundle),
        "required_files": REQUIRED_FILES,
        "optional_files": OPTIONAL_FILES,
        "missing_required": missing_required,
        "malformed": malformed,
        "found_optional": found_optional,
        "valid": not missing_required and not malformed,
    }

    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(f"Run bundle: {run_bundle}")
        print(f"Required files: {', '.join(REQUIRED_FILES)}")
        print(f"Optional files: {', '.join(OPTIONAL_FILES)}")
        if missing_required:
            print(f"Missing required: {', '.join(missing_required)}")
        if malformed:
            print("Malformed:")
            for item in malformed:
                print(f"- {item}")
        if found_optional:
            print(f"Found optional: {', '.join(found_optional)}")
        print(f"Valid: {summary['valid']}")

    return 0 if summary["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
