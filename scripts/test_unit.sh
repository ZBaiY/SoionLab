#!/usr/bin/env bash
set -euo pipefail

# 确保 src 在 PYTHONPATH
export PYTHONPATH="src:${PYTHONPATH:-}"

echo "[test_unit] Running unit tests..."
pytest -q tests/unit
