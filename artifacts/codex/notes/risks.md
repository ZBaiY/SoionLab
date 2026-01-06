# Risks / Failure Modes

- Orderbook layout assumption: `resolve_cleaned_paths(domain="orderbook")` expects `snapshot_YYYY-MM-DD.parquet`. If cleaned orderbook files use a different naming or partitioning, `has_local_data` will be false and workers will be skipped when `require_local_data=True`.
- Option-chain asset mapping: asset is normalized via `base_asset_from_symbol` after cfg/market selection. If on-disk layout uses a non-USDT asset code or a different naming convention, resolved paths will miss files.
- Symbol-domain normalization: ohlcv/orderbook/trades paths use `symbol_from_base_asset`, so a handler keyed by base asset (e.g., `BTC`) will look for `BTCUSDT` directories. If the cleaned layout is actually keyed by base asset for these domains, data will be skipped.
- Sentiment provider mapping: provider defaults to cfg `provider/source/venue`, else handler market venue (non-"unknown"), else symbol. If provider directory name differs, paths won't resolve.
- Domain naming mismatch risk: `IngestionTick.Domain` includes `"trade"` but plan conventions use `"trades"`. If trades ingestion is added to backtest plan without reconciliation, domain mismatch may surface.
- Missing data visibility: File sources now yield no ticks when no files exist (instead of raising). If `require_local_data=False`, missing data may be silent unless explicitly inspected.
- Resolver location drift: mitigated by centralizing in `src/quant_engine/utils/cleaned_path_resolver.py`, but any new path logic must route through this module.
- Deterministic ordering depends on resolver: When `paths` are provided, file sources do not re-sort; correctness relies on `resolve_cleaned_paths()` returning deterministic, chronological order.
