# Path Contracts

Authoritative resolver:
- `resolve_cleaned_paths()` in `src/quant_engine/utils/cleaned_path_resolver.py` is the source of truth for cleaned path layouts.
- `resolve_raw_paths()` in `src/quant_engine/utils/cleaned_path_resolver.py` mirrors cleaned layouts under `raw/`.
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
- Raw layouts mirror cleaned paths, with top-level `raw/` replacing `cleaned/`.

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

Runtime bootstrap/backfill contracts:
- Bootstrap (ALL modes): local-only preload of cached history. No external fetches, no gap fixing.
- Warmup (feature layer): checks handler history against `required_windows`.
  - BACKTEST: insufficient history raises immediately.
  - REALTIME/MOCK: triggers external backfill and rechecks before proceeding.
- Backfill (REALTIME/MOCK only): external-only fetch anchored to driver-provided `target_ts`.
  - Runtime wires backfill workers via `handler.set_external_source(worker, emit=...)`; handlers do not call source methods.
  - Ingestion worker owns `fetch_source` (external) + `raw_sink` (FileSource) and performs fetch → persist(raw) → emit tick.
  - Raw persistence must use FileSource writers; no direct filesystem writes in runtime/handlers.
  - Per-step gap check runs in `engine.align_to(ts)` before any step logic.
  - Gap detection uses `last_timestamp()` vs `target_ts - interval_ms`.
  - Cold-start backfill uses handler lookback to compute `[start_ts, target_ts]`.
  - iv_surface is derived: it skips external backfill and logs `iv_surface.backfill.skipped`.
