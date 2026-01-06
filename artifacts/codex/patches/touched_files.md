# Touched Files

- src/quant_engine/utils/apps_helpers.py
  - Use `resolve_cleaned_paths()` in `_build_backtest_ingestion_plan()`; derive intervals/asset/provider; pass `paths` into workers; add cfg indexing helper; fix `build_backtest_engine()` wiring.
- src/ingestion/ohlcv/source.py
  - `OHLCVFileSource` accepts optional `paths` and yields nothing if no files.
- src/ingestion/orderbook/source.py
  - `OrderbookFileSource` accepts optional `paths` and yields nothing if no files.
- src/ingestion/option_chain/source.py
  - `OptionChainFileSource` accepts optional `paths` and yields nothing if no files.
- src/ingestion/sentiment/source.py
  - `SentimentFileSource` accepts optional `paths` and yields nothing if no files when using explicit paths.
- src/ingestion/trades/source.py
  - `TradesFileSource` accepts optional `paths` for forward compatibility with resolver output.
- tests/integration/runtime/test_app_builders.py
  - Add `test_backtest_plan_detects_local_data()`.
- tests/resources/cleaned/ohlcv/BTCUSDT/15m/2024.parquet
  - Minimal fixture for backtest plan discovery.
