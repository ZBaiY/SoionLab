# Installation

## Supported environments
- OS: Ubuntu 22.04 LTS, macOS
- Python: 3.11, 3.12

## Option 1: venv + pip
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt && pip install -e .
```

## Option 2: conda/miniconda
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

## Editable install and import smoke test
```bash
pip install -e .
python -c "import quant_engine, ingestion; print('imports_ok')"
```

## Testing
```bash
pytest -q -m "not local_data" tests
```

Local/private dataset tests are opt-in:
```bash
pytest -q -m local_data tests
```
