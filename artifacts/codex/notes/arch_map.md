# Architecture Map

Backtest flow (relevant slice):
- `apps/run_backtest.py` calls `build_backtest_engine()`.
- `build_backtest_engine()` in `src/quant_engine/utils/apps_helpers.py`:
  - `StrategyCls.standardize()` -> returns `NormalizedStrategyCfg` (intervals standardized per domain).
  - `StrategyLoader.from_config()` -> returns `StrategyEngine` with per-domain handlers.
  - `_build_backtest_ingestion_plan()` -> builds a list of worker configs.
- `_build_backtest_ingestion_plan()`:
  - For each domain handler map on `StrategyEngine` (`ohlcv_handlers`, `orderbook_handlers`, `option_chain_handlers`, `sentiment_handlers`), compute resolved cleaned paths for `[start_ts, end_ts)` using `resolve_cleaned_paths()`.
  - Determine `has_local_data` by checking existence of any resolved paths.
  - Create `build_worker` closures that instantiate FileSource + Normalizer + Worker.

Ingestion components:
- File Sources read local cleaned files (synchronous iterators):
  - `OHLCVFileSource`, `OrderbookFileSource`, `OptionChainFileSource`, `SentimentFileSource`, `TradesFileSource`.
  - After changes: each accepts optional `paths: list[Path]` to read exact resolved files; otherwise uses internal glob/rglob.
- Workers (unchanged):
  - `OHLCVWorker`, `OrderbookWorker`, `OptionChainWorker`, `SentimentWorker`.
  - Convert raw records to `IngestionTick` via normalizers and emit into runtime queue.

Runtime boundary:
- `IngestionTick.Domain` in `src/ingestion/contracts/tick.py` defines allowed domain identifiers.
- `StrategyEngine` owns handlers and runtime step order; `BacktestDriver` pulls ticks and calls `engine.ingest_tick()`/`engine.step()`.

Key invariants:
- Runtime step order and determinism are untouched.
- `data_ts` is epoch ms (UTC). No local time conversions.
- Ingestion poll cadence does not control observation time.
