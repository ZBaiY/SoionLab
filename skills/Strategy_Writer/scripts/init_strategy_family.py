#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def repo_root_from_here() -> Path:
    return Path(__file__).resolve().parents[3]


def write_if_missing(path: Path, content: str) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize a strategy family layout.")
    parser.add_argument("--family", required=True, help="Family slug, e.g. rsi_adx_gateway")
    args = parser.parse_args()

    root = repo_root_from_here()
    family = args.family.strip()
    docs_slug = family

    family_root = root / "apps" / "strategy" / "families" / family
    test_path = root / "tests" / "unit" / "strategies" / f"test_{family}.py"
    docs_root = root / "docs" / "strategies" / docs_slug

    write_if_missing(family_root / "__init__.py", f'"""Strategy family: {family}."""\n')
    write_if_missing(
        test_path,
        f'"""Focused tests for the {family} strategy family."""\n',
    )
    write_if_missing(
        docs_root / "run_research.py",
        (
            "from __future__ import annotations\n\n"
            "from pathlib import Path\n"
            "import subprocess\n\n"
            'if __name__ == "__main__":\n'
            "    root = Path(__file__).resolve().parent\n"
            "    cfg = root / 'strategy_research_config.json'\n"
            "    subprocess.run([\n"
            "        'python3',\n"
            "        str(Path(__file__).resolve().parents[3] / 'skills' / 'Strategy_Writer' / 'scripts' / 'run_strategy_research.py'),\n"
            "        '--config',\n"
            "        str(cfg),\n"
            "    ], check=True)\n"
        ),
    )
    write_if_missing(
        docs_root / "strategy_research_config.json",
        (
            "{\n"
            f'  "output_dir": "{docs_root}",\n'
            '  "scan_strategy": "REPLACE-ME",\n'
            '  "base_bind": {"A": "BTCUSDT"},\n'
            '  "parameter_grid": {},\n'
            '  "references": [],\n'
            '  "is_windows": [],\n'
            '  "oos_windows": [],\n'
            f'  "report_title": "{family} research report"\n'
            "}\n"
        ),
    )

    print(f"Initialized family root: {family_root}")
    print(f"Initialized test file: {test_path}")
    print(f"Initialized docs root: {docs_root}")


if __name__ == "__main__":
    main()
