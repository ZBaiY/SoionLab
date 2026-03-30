import os
import random
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

# Allow plain `pytest tests` to resolve repo-local packages the same way the
# repo test scripts do via `PYTHONPATH=src`.
for path in (str(SRC_ROOT), str(REPO_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture(autouse=True)
def _seed_everything():
    random.seed(0)
    os.environ.setdefault("PYTHONHASHSEED", "0")
