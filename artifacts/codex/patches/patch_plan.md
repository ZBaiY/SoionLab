# Patch Plan

1. Add raw path mirror resolver and persistence hooks for ingestion FileSource classes (parquet/jsonl).
2. Implement worker-owned backfill (fetch → persist raw → emit tick) with wired fetch_source + raw_sink.
3. Rewire handlers/apps to use backfill workers (no runtime Source calls) and update tests for raw persistence + runtime isolation.
