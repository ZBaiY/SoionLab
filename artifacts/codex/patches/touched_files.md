# Touched Files

- src/quant_engine/utils/logger.py
  - Added step-trace category, category filters, robust `to_jsonable()`, and `log_step_trace()`.
- configs/logging.json
  - Added `trace` handler configuration with dedicated path.
- src/quant_engine/strategy/engine.py
  - Emit step-trace log after snapshot creation.
- tests/unit/test_step_trace_logging.py
  - New unit tests for serialization and trace sink writing.
