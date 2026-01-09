# Scope

Goal: add a dedicated per-step trace log sink (JSONL) with robust serialization and bounded payloads, while keeping existing logging and runtime semantics intact.

In-scope modules and functions:
- Logging config + utilities:
  - `src/quant_engine/utils/logger.py`
  - `configs/logging.json`
- Strategy step emission:
  - `src/quant_engine/strategy/engine.py`
- Tests:
  - `tests/unit/test_step_trace_logging.py`

Out of scope:
- New logging frameworks or class hierarchies.
- Changes to engine/driver ordering or strategy semantics.

Hard constraints honored:
- Trace logs are JSONL and routed to a dedicated sink.
- Serialization is robust and size-bounded (depth/items/strings).
- Market snapshot dumps are summarized by default, with opt-in full dumps via env.
