#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="src:${PYTHONPATH:-}"

echo "[test_all] Running all tests (unit + integration) - without local data..."
pytest -q -m "not local" tests