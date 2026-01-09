# Tick Routing Contamination Note

Root cause:
- `StrategyEngine.ingest_tick` broadcast ticks to all handlers in a domain, ignoring `tick.symbol`.
- Runtime handlers accepted any tick without validating `tick.domain` or `tick.symbol`.
- Ingestion tick construction is stable: `IngestionTick` is frozen and normalizers set domain/symbol correctly, so the async queue was not mutating ticks.

Fix:
- Route by both domain and symbol in `StrategyEngine.ingest_tick`, and drop unknown symbols with a log.
- Canonicalize domains (`trade` -> `trades`, `option_trade` -> `option_trades`) at the tick boundary.
- Add `source_id` to `IngestionTick` and propagate it from ingestion workers, then enforce `(domain, symbol, source_id)` in engine/handlers.

Regression tests:
- `tests/integration/test_tick_identity_preservation.py`
- `tests/integration/test_engine_ingest_routing_isolation.py`
- `tests/integration/test_handler_tick_guards.py`
- `tests/integration/test_cross_source_isolation.py`
