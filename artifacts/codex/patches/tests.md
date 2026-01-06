# Tests

Executed:
- python3 -m pytest tests/integration/runtime/test_app_builders.py -q
  - Result: 5 passed
- python3 -m pytest tests/runtime/test_ingestion_init_contract.py -q
  - Result: 7 passed

Not run:
- scripts/test_unit.sh
- scripts/test_all.sh
- scripts/lint.sh
- scripts/typecheck.sh
