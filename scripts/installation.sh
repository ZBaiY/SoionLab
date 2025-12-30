#!/usr/bin/env bash
set -euo pipefail

# Quant_Engine post-clone setup (VPS / Ubuntu 22.04)
# - Creates/updates a conda env
# - Installs deps (heavy deps via conda, the rest via pip)
# - Installs this repo in editable mode
# - Pins PYTHONPATH on env activation (repo-root anchored)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_NAME="${ENV_NAME:-qe}"
PY_VER="${PY_VER:-3.11}"

# ---- conda bootstrap -------------------------------------------------------
_has() { command -v "$1" >/dev/null 2>&1; }

_bootstrap_conda() {
  if _has conda; then
    # shellcheck disable=SC1090
    source "$(conda info --base)/etc/profile.d/conda.sh"
    return 0
  fi

  # Common root installs
  if [ -f "/root/miniconda3/etc/profile.d/conda.sh" ]; then
    # shellcheck disable=SC1091
    source "/root/miniconda3/etc/profile.d/conda.sh"
    return 0
  fi

  # Common user installs
  if [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
    # shellcheck disable=SC1091
    source "$HOME/miniconda3/etc/profile.d/conda.sh"
    return 0
  fi

  echo "ERROR: conda not found. Install Miniconda/Anaconda first." >&2
  exit 1
}

_accept_conda_tos_if_needed() {
  # Some conda versions require ToS acceptance for the default channels.
  # If the command doesn't exist or fails, we keep going.
  if conda tos -h >/dev/null 2>&1; then
    conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main >/dev/null 2>&1 || true
    conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r    >/dev/null 2>&1 || true
  fi
}

_bootstrap_conda
_accept_conda_tos_if_needed

# ---- env creation/update ---------------------------------------------------
if conda env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
  echo "[conda] Using existing env: $ENV_NAME"
else
  echo "[conda] Creating env: $ENV_NAME (python=$PY_VER)"
  conda create -n "$ENV_NAME" "python=$PY_VER" -y
fi

conda activate "$ENV_NAME"

# ---- deps ------------------------------------------------------------------
# Heavy deps via conda to avoid pip/conda ABI conflicts.
# Keep this list conservative.
conda install -y numpy pandas pyarrow scipy pytz matplotlib scikit-learn

python -m pip install -U pip

# Lightweight deps via pip.
python -m pip install \
  pydantic \
  aiohttp \
  websockets \
  requests \
  tqdm \
  python-dateutil \
  psutil \
  joblib \
  pytest \
  ta \
  python-binance

# ---- repo install (editable) ----------------------------------------------
cd "$REPO_ROOT"
pip install -U pip
pip install -e .
pip install -e ".[dev]"   # optional dev deps

# ---- env activation hook (PYTHONPATH) -------------------------------------
# Even with editable install, keeping PYTHONPATH repo-root anchored helps
# scripts that import from src-layout directly.
ACTIVATE_D="$CONDA_PREFIX/etc/conda/activate.d"
mkdir -p "$ACTIVATE_D"
cat > "$ACTIVATE_D/quant_engine_env.sh" <<SH
export PYTHONPATH="$REPO_ROOT/src"
SH

# ---- smoke checks ----------------------------------------------------------
python -c "import sys; import quant_engine, ingestion; print('ok:', sys.version.split()[0])"

if [ -f "$REPO_ROOT/scripts/path_sanity_check.py" ]; then
  python "$REPO_ROOT/scripts/path_sanity_check.py" || true
fi

echo
echo "[done] Env '$ENV_NAME' is ready."
echo "Activate with: source \"$(conda info --base)/etc/profile.d/conda.sh\" && conda activate $ENV_NAME"
echo "Run option chain ingestion (foreground):"
echo "  cd $REPO_ROOT && python apps/scrap/option_chain.py --asset BTC --intervals 1m,5m,1h"
