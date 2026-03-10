# Environment Variables Reference

This document collects the environment-variable surface currently used by the tracked codebase.

Scope:
- `src/`
- `apps/`
- `scripts/`
- `tests/`
- current audit runners under `docs/audits/.../scripts/`

Notes:
- This is a code-audit reference, not a `.env.example`.
- Variables are grouped by purpose.
- "Default" means the code path's built-in fallback when the variable is unset.
- Archived audit evidence files are not exhaustively repeated here.

## 1. Live Exchange Profile And Credentials

### `BINANCE_ENV`
- Scope: live runtime, Binance client resolution, some audit scripts, live tests
- Values: `testnet`, `mainnet`
- Default: `testnet`
- Comment: selects the Binance profile and therefore which credential env var names are required.

### `BINANCE_TESTNET_API_KEY`
- Scope: Binance testnet live runs and live-api tests
- Default: none
- Comment: required when `BINANCE_ENV=testnet`.

### `BINANCE_TESTNET_API_SECRET`
- Scope: Binance testnet live runs and live-api tests
- Default: none
- Comment: required when `BINANCE_ENV=testnet`.

### `BINANCE_MAINNET_API_KEY`
- Scope: Binance mainnet live runs
- Default: none
- Comment: required when `BINANCE_ENV=mainnet`.

### `BINANCE_MAINNET_API_SECRET`
- Scope: Binance mainnet live runs
- Default: none
- Comment: required when `BINANCE_ENV=mainnet`.

### `BINANCE_MAINNET_CONFIRM`
- Scope: live runtime and live matcher
- Required value: `YES`
- Default: unset
- Comment: hard safety gate. Mainnet startup and order submission are blocked unless this is explicitly set to `YES`.

### `BINANCE_BASE_URL`
- Scope: live runtime, Binance client
- Default:
  - testnet: `https://testnet.binance.vision`
  - mainnet: `https://api.binance.com`
- Comment: privileged override for the Binance REST base URL.

### `BINANCE_BASE_URL_CONFIRM`
- Scope: Binance client guard, realtime preflight
- Required value: `YES`
- Default: unset
- Comment: required to allow a custom `BINANCE_BASE_URL`, unless proxy mode is enabled.

### `BINANCE_PROXY_MODE`
- Scope: Binance client guard, realtime preflight
- Required value: `1`
- Default: unset
- Comment: alternate opt-in for `BINANCE_BASE_URL` override. Used when the process is intentionally behind a proxy/mirror.

### `LIVE_ALLOW_NONFLAT_START`
- Scope: realtime startup readiness gate
- Required value: `YES`
- Default: unset
- Comment: on mainnet, bypasses the flat-start requirement for the target symbol. Leave unset unless you intentionally want to start with existing inventory.

### `DERIBIT_BASE_URL`
- Scope: realtime app option-chain polling
- Default:
  - when Binance profile is testnet: `https://test.deribit.com`
  - otherwise: `https://www.deribit.com`
- Comment: explicit override for Deribit option-chain polling base URL.

### `DERIBIT_ENV`
- Scope: option-chain source fallback
- Values: typically `testnet`, `mainnet`
- Default: falls back to `BINANCE_ENV`, then `mainnet`
- Comment: controls Deribit environment selection when the option-chain source resolves its own profile.

## 2. Realtime Driver And Live Polling

These variables affect live runtime timing. They do not change strategy observation semantics; they only affect execution timing and fetch cadence.

### `REALTIME_STEP_DELAY_MS`
- Scope: realtime driver
- Default: `5000`
- Comment: post-close settle delay before a realtime step is processed. The semantic `step_ts` stays grid-aligned; this only delays wall-clock execution.

### `REALTIME_OHLCV_POLL_INTERVAL_MS`
- Scope: realtime OHLCV fetch source
- Default: `3000`
- Comment: REST polling cadence for OHLCV in live mode. This is IO cadence, not bar interval semantics.

### `REALTIME_OPTION_CHAIN_POLL_INTERVAL_MS`
- Scope: realtime option-chain fetch source
- Default: `5000`
- Comment: REST polling cadence for option-chain data in live mode. Only matters when option-chain polling is actually wired for the strategy/runtime.

## 3. Ingestion Writer Process And Memory Knobs

These control optional ingestion-side persistence helpers. They do not change domain semantics.

### `OHLCV_WRITE_PROCESS`
- Scope: OHLCV ingestion
- Values: truthy `1/true/yes/on`
- Default: `0`
- Comment: enables the optional OHLCV subprocess writer.

### `OHLCV_WRITE_QUEUE_SIZE`
- Scope: OHLCV ingestion
- Default: `64`
- Comment: queue capacity for the optional OHLCV writer subprocess.

### `OHLCV_WRITE_PUT_TIMEOUT_S`
- Scope: OHLCV ingestion
- Default: `5.0`
- Comment: max enqueue wait before surfacing writer backpressure as an error.

### `OPTION_CHAIN_WRITE_PROCESS`
- Scope: option-chain ingestion
- Values: truthy `1/true/yes/on`
- Default: `0`
- Comment: enables the optional option-chain subprocess writer.

### `OPTION_CHAIN_WRITER_RECYCLE_SECONDS`
- Scope: option-chain ingestion
- Default: source default
- Comment: writer-process recycle interval in seconds.

### `OPTION_CHAIN_WRITER_RECYCLE_TASKS`
- Scope: option-chain ingestion
- Default: source default
- Comment: writer-process recycle interval in task-count terms.

### `TRADES_WRITE_PROCESS`
- Scope: trades ingestion
- Values: truthy `1/true/yes/on`
- Default: `0`
- Comment: enables the optional trades subprocess writer.

### `TRADES_WRITE_QUEUE_SIZE`
- Scope: trades ingestion
- Default: source default
- Comment: queue capacity for the optional trades writer subprocess.

### `TRADES_WRITE_PUT_TIMEOUT_S`
- Scope: trades ingestion
- Default: source default
- Comment: max enqueue wait before surfacing trades-writer backpressure.

### `INGEST_MEMORY_TRIM_EVERY`
- Scope: periodic ingestion memory trim
- Default: `200`
- Comment: run best-effort GC/Arrow memory release every N ingestion steps. Set `0` to disable.

### `INGEST_MEMORY_TRIM_GEN`
- Scope: periodic ingestion memory trim
- Default: `2`
- Comment: GC generation used by the trim helper. Bounded to `0..2`.

## 4. Logging And Debug Observability

### `QUANT_TRACE_FULL_MARKET`
- Scope: trace/header/log summarization
- Values: truthy `1/true/yes/on`
- Default: off
- Comment: expands market details in trace/log output. Leave off unless you need fuller trace payloads.

### `ASYNCIO_HEARTBEAT_ENABLED`
- Scope: asyncio heartbeat diagnostics
- Values: truthy `1/true/yes/on`
- Default: off
- Comment: enables periodic `asyncio.health` log lines with loop lag and to-thread stats.

## 5. Data-Shape And Snapshot Behavior

### `OPTION_CHAIN_DROP_AUX`
- Scope: option-chain snapshot shaping
- Default: `true`
- Comment: controls whether auxiliary option-chain fields are dropped from the normalized snapshot by default.

## 6. Scraper-Specific Diagnostics

These are used by the committed option-chain scraper entrypoint.

### `MEMTRACE`
- Scope: `apps/scrap/option_chain.py`
- Values: truthy `1/true/yes/y/t`
- Default: `0`
- Comment: enables scraper memory-trace reporting.

### `TRACEMALLOC`
- Scope: `apps/scrap/option_chain.py`
- Values: truthy `1/true/yes/y/t`
- Default: `0`
- Comment: enables Python `tracemalloc` for the option-chain scraper.

### `MEMTRACE_EVERY`
- Scope: `apps/scrap/option_chain.py`
- Default: `3`
- Comment: sampling frequency for scraper memory diagnostics.

## 7. Test And CI Gates

### `LIVE_API_TEST`
- Scope: live-api integration tests
- Values: truthy presence
- Default: unset
- Comment: enables live API test selection. When unset, those tests skip by design.

### `PYTHONHASHSEED`
- Scope: test bootstrap helpers
- Default in test scripts: `0`
- Comment: test determinism helper. Set internally by test bootstrap scripts, but can also be set explicitly in the environment.

## 8. Current Audit Harness Variables

These are not core runtime knobs, but they are still active environment-controlled setups in tracked audit runners.

### `FULL_SCALE_SYMBOL`
- Scope: `docs/audits/.../run_live_workflow_3cycles.py`
- Default: `BTCUSDT`
- Comment: symbol override for the full-scale audit workflow.

### `FULL_SCALE_CYCLES`
- Scope: `docs/audits/.../run_live_workflow_3cycles.py`
- Default: `3`
- Comment: number of workflow cycles to run.

### `FULL_SCALE_FORCE_FIXTURE`
- Scope: `docs/audits/.../run_live_workflow_3cycles.py`
- Default: `0`
- Comment: forces fixture mode in the 3-cycle workflow.

### `FULL_SCALE_USE_LIVE_TESTNET`
- Scope: `docs/audits/.../run_live_workflow_3cycles.py`
- Default: `0`
- Comment: opt-in to using real Binance testnet credentials instead of fixture mode.

### `FULL_SCALE_V2_SYMBOL`
- Scope: V2 live audit runner
- Default: `BTCUSDT`
- Comment: V2-specific symbol override.

### `FULL_SCALE_V2_CYCLES`
- Scope: V2 live audit runner
- Default: `3`
- Comment: V2-specific cycle count.

### `FULL_SCALE_V2_FORCE_FIXTURE`
- Scope: V2 live audit runner
- Default: `0`
- Comment: V2-specific fixture override.

### `FULL_SCALE_V3_SYMBOL`
- Scope: V3 live audit runner
- Default: `BTCUSDT`
- Comment: V3-specific symbol override.

### `FULL_SCALE_V3_CYCLES`
- Scope: V3 live audit runner
- Default: `3`
- Comment: V3-specific cycle count.

### `FULL_SCALE_V3_FORCE_FIXTURE`
- Scope: V3 live audit runner
- Default: `0`
- Comment: V3-specific fixture override.

## 9. Recommended Minimal Live Setups

### Testnet live run

```bash
export BINANCE_ENV=testnet
export BINANCE_TESTNET_API_KEY=...
export BINANCE_TESTNET_API_SECRET=...
export REALTIME_OHLCV_POLL_INTERVAL_MS=3000
export REALTIME_STEP_DELAY_MS=5000
```

### Mainnet supervised run

```bash
export BINANCE_ENV=mainnet
export BINANCE_MAINNET_API_KEY=...
export BINANCE_MAINNET_API_SECRET=...
export BINANCE_MAINNET_CONFIRM=YES
export REALTIME_OHLCV_POLL_INTERVAL_MS=3000
export REALTIME_STEP_DELAY_MS=5000
```

Optional:

```bash
export LIVE_ALLOW_NONFLAT_START=YES
```

Only use that override if a non-flat startup is intentional.

## 10. Operational Notes

- `interval` and `poll_interval_ms` are intentionally different concepts.
  - `interval` is semantic observation time.
  - `poll_interval_ms` is fetch cadence.
- Faster OHLCV polling does not automatically mean denser OHLCV parquet history.
  - OHLCV raw persistence is gated by new closed-bar identity, not by every poll.
- Snapshot-style domains such as option chain can be more sensitive to poll cadence if you later choose to tune them aggressively.
