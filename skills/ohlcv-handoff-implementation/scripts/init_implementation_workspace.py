#!/usr/bin/env python3
from __future__ import annotations

import argparse
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


def read_template(name: str) -> str:
    template_path = Path(__file__).resolve().parent / "templates" / name
    return template_path.read_text(encoding="utf-8")


def write_if_missing(path: Path, content: str, force: bool) -> None:
    if path.exists() and not force:
        return
    path.write_text(content, encoding="utf-8")


def build_template_context(run_bundle: Path) -> dict[str, str]:
    return {
        "RUN_BUNDLE_PATH": str(run_bundle),
        "IMPLEMENTATION_PATH": str(run_bundle / "implementation"),
    }


def render(template: str, context: dict[str, str]) -> str:
    result = template
    for key, value in context.items():
        result = result.replace("{{" + key + "}}", value)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize a run-local implementation workspace.")
    parser.add_argument("--run-bundle", required=True, help="Path to one OHLCV research run bundle.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing implementation files.",
    )
    args = parser.parse_args()

    run_bundle = resolve_path(args.run_bundle)
    impl_dir = run_bundle / "implementation"
    impl_dir.mkdir(parents=True, exist_ok=True)

    context = build_template_context(run_bundle)
    template_map = {
        "implementation_plan.md": "implementation_plan.md.tmpl",
        "implementation_progress.md": "implementation_progress.md.tmpl",
        "implementation_report.md": "implementation_report.md.tmpl",
        "test_report.md": "test_report.md.tmpl",
        "changed_files.md": "changed_files.md.tmpl",
    }

    for out_name in REQUIRED_IMPL_FILES:
        template_name = template_map[out_name]
        content = render(read_template(template_name), context)
        write_if_missing(impl_dir / out_name, content, force=args.force)

    print(f"Initialized implementation workspace: {impl_dir}")
    for out_name in REQUIRED_IMPL_FILES:
        print(f"- {impl_dir / out_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
