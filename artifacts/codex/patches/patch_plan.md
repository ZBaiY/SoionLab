# Patch Plan

1. Replace ad-hoc cleaned path construction in `_build_backtest_ingestion_plan()` with `resolve_cleaned_paths()` and use strategy-driven intervals and per-domain metadata.
2. Pass resolved path lists into FileSource constructors to avoid duplicated layout logic and to keep IO deterministic.
3. Add a minimal integration test fixture and assertion that a backtest plan detects local cleaned data.
