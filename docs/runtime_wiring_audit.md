<!-- PR CHECKLIST:
- Checked: driver lifecycle ordering, engine step order, handler/model/decision/risk/execution/portfolio contracts, timestamp propagation.
- Fixed: protocol wiring + lifecycle entrypoints + stage guard; handler warmup/load_history stubs; driver shutdown/get_snapshot decoupling.
- Unchanged: StrategyEngine module placement and handler bootstrap no-op data loading.
-->
# Runtime Wiring Audit

## Scope and invariants checked
- Strategy spec vs loader assembly vs engine runtime semantics.
- Driver lifecycle ordering for backtest and realtime/mock.
- Step ordering: handlers → features → models → decision → risk → execution → portfolio → snapshot.
- Epoch-ms timestamp propagation in engine and snapshots.
- Protocol-based wiring between layers.

## Observed issues / ambiguities
- Engine depended on concrete classes and lacked `get_snapshot`/`last_timestamp` interfaces; driver reached into engine internals for shutdown and portfolio state (`src/quant_engine/strategy/engine.py`, `src/quant_engine/runtime/driver.py`).
- Backtest/realtime lifecycle used `preload_data` directly (bootstrap/load_history semantics unclear) (`src/quant_engine/runtime/backtest.py`, `src/quant_engine/runtime/realtime.py`).
- No explicit step-order guard; portfolio reads occurred without a stage-order check (`src/quant_engine/strategy/engine.py`).
- Handler protocol referenced warmup/load_history but handlers only exposed align_to/bootstrap (`src/quant_engine/data/contracts/protocol_realtime.py`).
- Mock driver lacked a runtime loop (`src/quant_engine/runtime/mock.py`).

## Fixes applied
- Added protocol types for engine/feature/execution/risk wiring and updated engine/driver to depend on protocols (`src/quant_engine/contracts/engine.py`, `src/quant_engine/contracts/feature.py`, `src/quant_engine/contracts/execution/engine.py`, `src/quant_engine/contracts/risk.py`, `src/quant_engine/runtime/driver.py`, `src/quant_engine/strategy/engine.py`, `src/quant_engine/features/extractor.py`).
- Added `bootstrap`/`load_history` engine entrypoints and updated drivers to call them (`src/quant_engine/strategy/engine.py`, `src/quant_engine/runtime/realtime.py`, `src/quant_engine/runtime/backtest.py`, `src/quant_engine/runtime/bootstrap.py`).
- Added `load_history`/`warmup_to` stubs to handlers to align with the contract (`src/quant_engine/data/ohlcv/realtime.py`, `src/quant_engine/data/orderbook/realtime.py`, `src/quant_engine/data/derivatives/option_chain/chain_handler.py`, `src/quant_engine/data/derivatives/iv/iv_handler.py`, `src/quant_engine/data/sentiment/sentiment_handler.py`, `src/quant_engine/data/trades/realtime.py`, `src/quant_engine/data/derivatives/option_trades/realtime.py`).
- Implemented step-order guard, snapshot copying, and timestamp enforcement (`src/quant_engine/strategy/engine.py`, `src/quant_engine/runtime/snapshot.py`, `src/quant_engine/contracts/portfolio.py`).
- Implemented mock driver run loop and decoupled driver shutdown via engine iterator (`src/quant_engine/runtime/mock.py`, `src/quant_engine/runtime/driver.py`).
- Added engine init wiring log line (`src/quant_engine/strategy/engine.py`).

## Guards / tests added
- Contract guards for handlers/components and step-order guard (`src/quant_engine/strategy/engine.py`).
- Wiring tests for step order and backtest/realtime lifecycle (`tests/runtime/test_runtime_wiring_contracts.py`).

## Remaining risks / intentional deviations
- StrategyEngine remains in `src/quant_engine/strategy/` and imports runtime modules; relocating it is a larger refactor.
- Handler `load_history` remains no-op; backtest still relies on ingestion replay rather than handler-side history loading.
- Snapshot copying is shallow; nested mutable payloads inside `market_data` can still be mutated by callers.

## Verdict
- Wiring now satisfies the stated invariants with explicit lifecycle entrypoints, protocol-based interfaces, stage-order guardrails, and deterministic timestamp handling. Remaining deviations are documented and localized.
