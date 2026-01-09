# Architecture Map

Logging sinks:
- Default log sink: `artifacts/runs/{run_id}/logs/{mode}.jsonl` (unchanged).
- Trace sink: `artifacts/runs/{run_id}/logs/{mode}/trace.jsonl` (JSONL, category-filtered).

Trace log entrypoint:
- `log_step_trace()` in `src/quant_engine/utils/logger.py`:
  - attaches `category="step_trace"`
  - sanitizes payload via `to_jsonable()`
  - summarizes market snapshots unless `QUANT_TRACE_FULL_MARKET=1`

Emission point:
- `StrategyEngine.step()` calls `log_step_trace(...)` after `EngineSnapshot` is created.

Filtering behavior:
- `CategoryFilter` routes `step_trace` records only to the trace handler and excludes them from console/default file.
- JSON formatter lifts `run_id`, `mode`, `strategy`, and `symbol` to top level for trace records and uses `step_ts` as the record `ts`.
