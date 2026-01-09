# Patch Plan

1. Add a step-trace log category and dedicated trace file handler with category filters.
2. Implement `to_jsonable()` and `log_step_trace()` with truncation and market snapshot summaries.
3. Emit trace logs from `StrategyEngine.step()` and add unit tests for serialization and sink writing.
