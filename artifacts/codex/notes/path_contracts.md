# Path Contracts

Log sinks:
- Default logs: `artifacts/runs/{run_id}/logs/{mode}.jsonl`
- Step trace logs: `artifacts/runs/{run_id}/logs/{mode}/trace.jsonl`

Trace payload control:
- `QUANT_TRACE_FULL_MARKET=1` enables full market snapshot dumps.
- Default behavior summarizes `market_snapshots` to keys + timestamp fields + small numeric sample.
- `primary_snapshots` are always included (sanitized).
