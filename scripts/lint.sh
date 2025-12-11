#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="src:${PYTHONPATH:-}"

echo "[lint] Running ruff on src/ and tests/..."
ruff check src tests --ignore F401,F841
# TODO: Remove ignore rules once scaffolding phase is complete
