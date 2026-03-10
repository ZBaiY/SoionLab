#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="src:${PYTHONPATH:-}"

PRIVATE_STRATEGY_TEST_DIR="tests/unit/strategies"

PRIVATE_STRATEGY_K_EXPR="not test_loader_builds_rsi_iv_without_model \
and not test_loader_builds_rsi_iv_fractional_without_model \
and not test_loader_builds_rsi_iv_dynamical_adx_sideway_without_model \
and not test_loader_builds_rsi_iv_dynamical_adx_sideway_fractional_without_model \
and not test_loader_builds_rsi_iv_relative_state_adx_sideway_fractional_without_model"

echo "[test_all] Running all tests (unit + integration) - without local data..."
pytest -q -m "not local" \
  --ignore="${PRIVATE_STRATEGY_TEST_DIR}" \
  -k "${PRIVATE_STRATEGY_K_EXPR}" \
  tests
