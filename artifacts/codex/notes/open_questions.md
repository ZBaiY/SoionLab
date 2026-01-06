# Open Questions

1. Orderbook cleaned layout: is the canonical pattern `cleaned/orderbook/<SYMBOL>/snapshot_YYYY-MM-DD.parquet`, or is a different partitioning expected (yearly, intraday, etc.)?
2. Option-chain asset mapping: should the asset directory always be the handler symbol, or should it come from a specific config field (e.g., `asset` vs `currency` vs `underlying`)?
3. Sentiment provider: should provider directories be keyed by data source (e.g., "news", "reddit") rather than symbol? If so, where should this be specified in strategy config?
4. Trades / option_trades / iv_surface: should backtest ingestion plan include these domains? If yes, is there a canonical FileSource for option_trades/iv_surface that matches `resolve_cleaned_paths()`?
5. Resolver placement: spec mentions `src/quant_engine/utils/cleaned_path_resolver.py`, but resolver currently lives in `apps_helpers.py`. Should it be moved or re-exported to a dedicated module?
