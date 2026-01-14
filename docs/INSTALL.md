# Installation

This repository contains a self-contained runtime instance used for research and experimentation.

- Runtime data root: `./data/`
- Runtime artifacts root: `./artifacts/`

All filesystem paths are repo-root anchored (no CWD dependence).

## Quick start (research environment)
The following setup is intended for a controlled research environment (e.g., workstation or VPS).

```bash
apt-get update && apt-get install -y curl bzip2
curl -fsSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -p /root/miniconda3
/root/miniconda3/bin/conda init bash
# reopen shell or: source ~/.bashrc

bash scripts/installation.sh
source /root/miniconda3/etc/profile.d/conda.sh
conda activate qe
```

## Alternative (no conda): venv
```bash
apt-get update && apt-get install -y python3-venv python3-dev build-essential
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
pip install -e .
python -c "import quant_engine, ingestion; print('imports_ok')"
```

## Testing
CI runs unit and integration tests without relying on local or private datasets.

```bash
pytest -q -m "not local_data" tests
```

Local/private dataset tests are opt-in:
- mark with `@pytest.mark.local_data`
- run with: `pytest -q -m local_data tests`
