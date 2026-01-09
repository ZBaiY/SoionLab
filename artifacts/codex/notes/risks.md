# Risks / Failure Modes

- Trace payload size: even with truncation, large feature/model payloads can create heavy trace logs. Mitigation: depth/items/string caps and market snapshot summaries.
- Trace/category filtering: if a caller sets a wrong category or bypasses `log_step_trace`, records may leak into the default log or miss the trace sink.
- Step timestamp vs log timestamp: trace logs use `step_ts` as `ts`; log emission time is stored in `log_ts`/`log_ts_ms` only for trace records.
- Full market dumps: `QUANT_TRACE_FULL_MARKET=1` can produce large log volumes; should be enabled only for targeted debugging.
