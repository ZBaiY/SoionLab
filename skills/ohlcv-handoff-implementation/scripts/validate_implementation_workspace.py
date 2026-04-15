#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


REQUIRED_IMPL_FILES = [
    "implementation_plan.md",
    "implementation_progress.md",
    "implementation_report.md",
    "test_report.md",
    "changed_files.md",
]


def repo_root_from_here() -> Path:
    return Path(__file__).resolve().parents[3]


def resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else (repo_root_from_here() / path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate a run-local implementation workspace.")
    parser.add_argument("--run-bundle", required=True, help="Path to one OHLCV research run bundle.")
    parser.add_argument("--json", action="store_true", help="Emit JSON summary instead of text.")
    args = parser.parse_args()

    run_bundle = resolve_path(args.run_bundle)
    impl_dir = run_bundle / "implementation"
    missing: list[str] = []
    empty: list[str] = []

    if not impl_dir.exists():
        missing = list(REQUIRED_IMPL_FILES)
    else:
        for rel in REQUIRED_IMPL_FILES:
            path = impl_dir / rel
            if not path.exists():
                missing.append(rel)
            elif not path.read_text(encoding="utf-8").strip():
                empty.append(rel)

    summary = {
        "run_bundle": str(run_bundle),
        "implementation_dir": str(impl_dir),
        "required_files": REQUIRED_IMPL_FILES,
        "missing": missing,
        "empty": empty,
        "valid": impl_dir.exists() and not missing and not empty,
    }

    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(f"Implementation dir: {impl_dir}")
        if missing:
            print(f"Missing: {', '.join(missing)}")
        if empty:
            print(f"Empty: {', '.join(empty)}")
        print(f"Valid: {summary['valid']}")

    return 0 if summary["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
