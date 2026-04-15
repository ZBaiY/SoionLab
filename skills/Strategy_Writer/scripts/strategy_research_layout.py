from __future__ import annotations

from pathlib import Path


def main() -> None:
    root = Path("/Users/zhaoyub/Documents/Tradings/SoionLab")
    print("Recommended strategy research layout:\n")
    print(root / "apps/strategy/families/<family_slug>/__init__.py")
    print(root / "apps/strategy/families/<family_slug>/<module>.py")
    print(root / "apps/strategy/<compatibility_view>.py")
    print(root / "tests/unit/strategies/test_<family_slug>.py")
    print(root / "docs/strategies/<strategy_slug>/run_research.py")
    print(root / "docs/strategies/<strategy_slug>/strategy_research_config.json")
    print(root / "docs/strategies/<strategy_slug>/research_results.json")
    print(root / "docs/strategies/<strategy_slug>/parameter_scan.csv")
    print(root / "docs/strategies/<strategy_slug>/<strategy_slug>_research_report.md")
    print(root / "skills/Strategy_Writer/scripts/run_strategy_research.py")
    print(root / "skills/Strategy_Writer/scripts/common_research.py")


if __name__ == "__main__":
    main()
