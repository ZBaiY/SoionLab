# Risks / Failure Modes

- Orderbook layout assumption: `resolve_cleaned_paths(domain="orderbook")` expects `snapshot_YYYY-MM-DD.parquet`. If cleaned orderbook files use a different naming or partitioning, `has_local_data` will be false and workers will be skipped when `require_local_data=True`.
- Option-chain asset mapping: asset is normalized via `base_asset_from_symbol` after cfg/market selection. If on-disk layout uses a non-USDT asset code or a different naming convention, resolved paths will miss files.
- Symbol-domain normalization: ohlcv/orderbook/trades paths use `symbol_from_base_asset`, so a handler keyed by base asset (e.g., `BTC`) will look for `BTCUSDT` directories. If the cleaned layout is actually keyed by base asset for these domains, data will be skipped.
- Sentiment provider mapping: provider defaults to cfg `provider/source/venue`, else handler market venue (non-"unknown"), else symbol. If provider directory name differs, paths won't resolve.
- Domain naming mismatch risk: `IngestionTick.Domain` includes `"trade"` but plan conventions use `"trades"`. If trades ingestion is added to backtest plan without reconciliation, domain mismatch may surface.
- Missing data visibility: File sources now yield no ticks when no files exist (instead of raising). If `require_local_data=False`, missing data may be silent unless explicitly inspected.
- Resolver location drift: mitigated by centralizing in `src/quant_engine/utils/cleaned_path_resolver.py`, but any new path logic must route through this module.
- Deterministic ordering depends on resolver: When `paths` are provided, file sources do not re-sort; correctness relies on `resolve_cleaned_paths()` returning deterministic, chronological order.
- Backfill drift pitfall: using wall-clock to set backfill end times can cause drift and nondeterministic gaps under load. Mitigation: per-step backfill is anchored to driver-provided `target_ts` and never extends it.
- External backfill dependency: backfill requires a configured ingestion worker with both `fetch_source` and `raw_sink`; if either is missing, gaps remain and warmup/step will fail in realtime/mock.
- Worker wiring: runtime/apps must attach the ingestion worker to handlers via `set_external_source(worker, emit=...)`; otherwise backfill silently no-ops (logged) even if data exists externally.
- Raw persistence layout: raw writers now mirror cleaned layouts. If your existing raw data uses a different partitioning, you may see duplicate or unexpected files under `raw/`.
- Raw schema drift: raw persistence uses append-only parquet writers; schema mismatches across backfill payloads will raise write errors.
- IV surface skip: iv_surface intentionally skips external backfill; if required windows include iv_surface without sufficient local data, warmup will still fail.
- Backtest warmup strictness: BACKTEST now fails when local history is insufficient (no auto-backfill). Ensure fixtures or local cleaned data exist for required windows.
- Realtime backfill concurrency: `align_to` can now run in a thread to avoid blocking the event loop; if ingestion tasks mutate handler caches concurrently, subtle races are possible. Mitigation: keep backfill minimal and driver-anchored; avoid additional concurrent writes.
