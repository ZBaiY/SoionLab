# Path Contracts

Authoritative resolver:
- `resolve_cleaned_paths()` in `src/quant_engine/utils/cleaned_path_resolver.py` is the source of truth for cleaned path layouts.
- Normalization helpers (pure, no IO):
  - `base_asset_from_symbol(symbol: str) -> str` strips `USDT` suffix when present.
  - `symbol_from_base_asset(asset: str, quote: str = "USDT") -> str` appends `USDT` when missing.
- All timestamps are epoch ms UTC; `end_ts` is exclusive.
- `data_root` is the parent of `cleaned/` (typically `<repo>/data`).

Resolved layouts (current):
- ohlcv: `cleaned/ohlcv/<SYMBOL>/<INTERVAL>/<YEAR>.parquet`
  - Uses `_iter_years_utc(start_ts, end_ts)`.
- orderbook: `cleaned/orderbook/<SYMBOL>/snapshot_<YYYY-MM-DD>.parquet`
  - Uses `_iter_days_utc(start_ts, end_ts)`.
- trades: `cleaned/trades/<SYMBOL>/<YEAR>/<MM>/<DD>.parquet`
  - Uses `_iter_days_utc(start_ts, end_ts)`.
- option_chain: `cleaned/option_chain/<ASSET>/<INTERVAL>/<YEAR>/<YYYY_MM_DD>.parquet`
  - Uses `_iter_days_utc(start_ts, end_ts)`.
- option_trades: `cleaned/option_trades/<VENUE>/<ASSET>/<YEAR>/<YYYY_MM_DD>.parquet`.
- iv_surface: placeholder daily under `cleaned/iv_surface/<ASSET>/<INTERVAL>/<YEAR>/<YYYY_MM_DD>.parquet`.
- sentiment: `cleaned/sentiment/<PROVIDER>/<YEAR>/<MM>/<DD>.jsonl`.

Plan-to-source contracts:
- `_build_backtest_ingestion_plan()` computes resolved path lists per domain and passes them to `FileSource` via `paths=` to avoid duplicated layout logic.
- `has_local_data` is true when any resolved path exists (unless `require_local_data=False`, which forces true).

Domain-specific metadata selection in plan:
- Interval is derived from handler (`handler.interval`) or normalized cfg block (`cfg.data.*.*.interval`) as fallback.
- Symbol domains (ohlcv/orderbook/trades) resolve paths using `symbol_from_base_asset(symbol)` to default to `...USDT` directories.
- Asset domains (option_chain/option_trades/iv_surface) resolve paths using `base_asset_from_symbol(...)` from cfg values or handler symbol.
- Sentiment provider is derived from cfg fields (`provider`, `source`, `venue`), else handler market venue (if not "unknown"), else symbol.

File source behavior with explicit paths:
- `OHLCVFileSource`, `OrderbookFileSource`, `OptionChainFileSource`, `SentimentFileSource`, `TradesFileSource` accept `paths: list[Path]`.
- Each source resolves non-absolute paths under the given `root` and only reads existing files.
- If the resolved list is empty, the iterator yields nothing (no exception).
- Without `paths`, sources fall back to their legacy glob-based layouts and may raise if strict and empty (sentiment) or if path missing (ohlcv/orderbook/option_chain).
