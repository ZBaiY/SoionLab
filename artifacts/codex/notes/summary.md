Summary of recent logic changes

- OHLCV closed-bar visibility: `src/quant_engine/data/ohlcv/realtime.py` now clamps reads to closed bars with a two-candidate boundary (exact boundary then boundary-1), and warns if returned bars exceed the effective visible end.
- Backtest determinism barrier: `src/quant_engine/runtime/backtest.py` waits on an OHLCV watermark derived from ingested OHLCV ticks (`StrategyEngine._last_tick_ts_by_key`) and only errors when ingestion tasks are done and watermark is still behind.
- Step trace diagnostics: `src/quant_engine/strategy/engine.py` and `src/quant_engine/runtime/backtest.py` emit expected visible end, actual last timestamp, and closed-bar readiness for BACKTEST steps.
- StopReplay control-flow: `OptionChainWorker` and `OHLCVWorker` treat `_StopReplay` as a clean stop (INFO log) without emit_error or hanging.
- IV surface logging: `iv_surface.backfill.skipped` downgraded to INFO unless `bootstrap.backfill_required` is set.
- Test tooling: backtest stress test now uses engine timestamp (not wall clock), filters by run_id, and prints detailed readiness/divergence diagnostics; replay probe test no longer hangs because probe task is cancelled when a worker completes.
