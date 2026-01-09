# Open Questions

1. Should `EngineSpec` carry an explicit strategy name so trace logs can always populate `strategy`?
2. Should trace logging be gated by a config flag to reduce overhead in high-frequency runs?
3. Should `market_snapshots` summary include a fixed schema per domain (e.g., always `data_ts` + `mid`/`price`), or is the generic summary sufficient?
