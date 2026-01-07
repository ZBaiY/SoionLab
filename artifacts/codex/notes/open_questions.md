# Open Questions

1. Orderbook cleaned layout: is the canonical pattern `cleaned/orderbook/<SYMBOL>/snapshot_YYYY-MM-DD.parquet`, or is a different partitioning expected (yearly, intraday, etc.)?
2. Option-chain asset mapping: should the asset directory always be the handler symbol, or should it come from a specific config field (e.g., `asset` vs `currency` vs `underlying`)?
3. Sentiment provider: should provider directories be keyed by data source (e.g., "news", "reddit") rather than symbol? If so, where should this be specified in strategy config?
4. Trades / option_trades / iv_surface: should backtest ingestion plan include these domains? If yes, should we add cleaned fixtures and explicit FileSource wiring for option_trades/iv_surface?
5. Raw schema ownership: do we need explicit schema versioning for raw orderbook/trades writes to prevent drift across backfill payloads?
