#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="src:${PYTHONPATH:-}"

echo "[test_all] Running all tests (unit + integration) - without local data..."
pytest -q -m "not local" \
  --ignore=tests/unit/test_rsi_iv_dynamical_adx_sideway.py \
  --ignore=tests/unit/test_rsi_iv_relative_state_adx_sideway.py \
  --ignore=tests/unit/test_rsi_iv_strategy_v1.py \
  tests
