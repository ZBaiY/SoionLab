# Scope

Goal: make backtest ingestion plan use the pure resolver `resolve_cleaned_paths()` to produce correct cleaned-data paths per domain, and align FileSource path consumption with that resolver output. Keep runtime semantics unchanged and preserve determinism.

In-scope modules and functions (minimal touch):
- `src/quant_engine/utils/apps_helpers.py`
  - `resolve_cleaned_paths()` (used as authoritative layout resolver)
  - `_build_backtest_ingestion_plan()` (plan construction)
  - `build_backtest_engine()` (wiring; pass normalized cfg context into plan)
- File sources used by backtest ingestion:
  - `src/ingestion/ohlcv/source.py` -> `OHLCVFileSource`
  - `src/ingestion/orderbook/source.py` -> `OrderbookFileSource`
  - `src/ingestion/option_chain/source.py` -> `OptionChainFileSource`
  - `src/ingestion/sentiment/source.py` -> `SentimentFileSource`
  - `src/ingestion/trades/source.py` -> `TradesFileSource` (compat path list support)

Out of scope:
- Strategy definitions or handler logic (no semantic changes).
- Engine/Driver lifecycle, step ordering, or StrategyBase standardization.
- Architecture refactors; only glue and path resolution.

Hard constraints honored:
- Use strategy/handler intervals from `engine.*_handlers` or normalized cfg; no hardcoded intervals.
- Do not change runtime lifecycle or determinism.
- No heavy IO scanning; check only resolved path list for existence.
- If no files exist, FileSource yields no ticks and must not crash.
