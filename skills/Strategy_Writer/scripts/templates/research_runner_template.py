from __future__ import annotations

"""Template note for strategy-family docs-local research entrypoints."""

from pathlib import Path
import subprocess


def main() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    docs_root = Path(__file__).resolve().parents[3] / "docs/strategies/<strategy_slug>"
    config_path = docs_root / "strategy_research_config.json"
    runner = repo_root / "skills/Strategy_Writer/scripts/run_strategy_research.py"
    subprocess.run(["python3", str(runner), "--config", str(config_path)], check=True)


if __name__ == "__main__":
    main()
