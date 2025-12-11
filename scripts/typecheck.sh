#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="src:${PYTHONPATH:-}"

echo "[mypy] Type checking src/quant_engine ..."
mypy src/quant_engine
