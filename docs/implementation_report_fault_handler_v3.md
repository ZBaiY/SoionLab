# Fault/Health Control Plane v3 — Implementation Report

**Date:** 2026-03-03  
**Implementation target:**  
- `docs/audits/fault_handler/implementation_plan_v3.md`  
- `docs/audits/fault_handler/design_v3.md`

## 1. Executive Summary

The v3 health system has been implemented end-to-end across core health modules, strategy/runtime wiring, async ingestion fault routing, feature-channel isolation, and restart scheduling hooks.  

Implemented scope includes:
- New `quant_engine.health` package with event/action contracts, config, state machines, policy, manager, session helper, restart manager, and exports.
- Strategy/driver integration so fault decisions are emitted via `HealthManager` and applied at intercept points.
- Engine step-stage isolation (`features`, `models`, `decision`, `risk`, `execution`, `portfolio`) with health-driven fallback behavior.
- Realtime readiness and staleness escalation path via `HealthManager`.
- Async task callback routing through health actions (`HALT`, `RESTART_SOURCE`, isolation path).
- Feature-channel exception isolation (fail-static behavior for channel failures).
- Health snapshot embedded in `EngineSnapshot`.

## 2. Deliverables by Plan Phase

## Phase 0 (Foundation)

Completed:
- Added new package `src/quant_engine/health/`:
  - `events.py`
  - `config.py`
  - `snapshot.py`
  - `state.py`
  - `session.py`
  - `policy.py`
  - `manager.py`
  - `restart.py`
  - `__init__.py`
- Added `HealthSnapshot` support in runtime snapshot model.
- Added config presets:
  - `default_realtime_config(interval_ms=...)`
  - `backtest_config(interval_ms=...)`

Key implemented contracts:
- `FaultEvent`, `FaultKind`, `Action`, `ActionKind`, `ExecutionPermit`
- `DomainPolicyCfg`, `FaultPolicyCfg`
- `DomainHealthState`, `GlobalSafetyMode`, `HealthSnapshot`, `DomainHealthSummary`
- `DomainHealth` + `GlobalSafety` state machines
- `FaultPolicy.evaluate(...)`
- `HealthManager` public surface

## Phase 1 (Engine Step Isolation)

Completed in `src/quant_engine/strategy/engine.py`:
- Added `health: HealthManager | None` to `StrategyEngine` constructor.
- Added `StepFallback` and `_apply_step_action(...)`.
- Added `_build_skip_snapshot(...)` (with health embedding when active).
- Wrapped step stages with fault intercepts:
  - `engine.step.features`
  - `engine.step.model.<name>`
  - `engine.step.decision`
  - `engine.step.risk`
  - `engine.step.execution`
  - `engine.step.portfolio`
- Implemented execution permit handling (`FULL`, `REDUCE_ONLY`, `BLOCK`) and safe-mode overrides.
- Calls `report_step_ok(...)` at end of successful step.
- Calls `report_step_skipped(...)` when skip-snapshot fallback is taken.

## Phase 2 (Ingestion/Realtime Boundary)

Completed:
- `engine.ingest_tick(...)` now catches handler exceptions and reports `HANDLER_EXCEPTION`.
- Successful tick ingestion reports liveness via `report_tick(domain, symbol, ts)`.
- Realtime readiness gate now reports skipped steps and can halt via health escalation.
- Realtime loop invokes `check_staleness(ts)` and halts on `ActionKind.HALT`.

Files:
- `src/quant_engine/strategy/engine.py`
- `src/quant_engine/runtime/realtime.py`

## Phase 3 (Feature Isolation + Restart Hooks)

Completed:
- Per-channel exception isolation in `FeatureExtractor` (`initialize`, `warmup`, `update`) with health reporting.
- Added `SourceRestartManager` implementation.
- Added async task health routing and restart callback hook in `create_task_named(...)`.
- Realtime app ingestion now uses `create_task_named(...)` with health domain/symbol metadata and restart scheduling callback.

Files:
- `src/quant_engine/features/extractor.py`
- `src/quant_engine/health/restart.py`
- `src/quant_engine/utils/asyncio.py`
- `apps/run_realtime.py`

## 3. Wiring and Integration Changes

### Loader and Engine Construction

- `src/quant_engine/strategy/loader.py`
  - Constructs health config by mode:
    - realtime/mock: `default_realtime_config(...)`
    - backtest/sample: `backtest_config(...)`
  - Instantiates `HealthManager`.
  - Injects health into `FeatureExtractor` and `StrategyEngine`.

### Snapshot Embedding

- `src/quant_engine/runtime/snapshot.py`
  - Added `health: HealthSnapshot | None`.
  - Included `health` in `to_dict()`.

### Driver Base

- `src/quant_engine/runtime/driver.py`
  - Added optional health field on driver (`self._health`), defaulting to engine-attached health.

## 4. Test Additions

New tests added under `tests/unit/quant_engine/health/`:
- `test_state_machines.py`
- `test_policy.py`
- `test_session.py`
- `test_manager.py`
- `test_action_coverage.py`

Coverage focus:
- Domain transition semantics including probing/backoff.
- Guardrail HALT behavior.
- Unknown domain/kind conservative handling.
- Severity hint escalation/no-downgrade.
- Execution uncertainty path (`REDUCE_ONLY` immediate hold).
- Health manager snapshot/report behavior.
- ActionKind dispatch coverage for step helper.

## 5. Validation Evidence

Executed validation commands (all pass):

1. `ruff check src/quant_engine/health src/quant_engine/strategy/engine.py src/quant_engine/strategy/loader.py src/quant_engine/runtime/realtime.py src/quant_engine/runtime/snapshot.py src/quant_engine/utils/asyncio.py src/quant_engine/features/extractor.py src/quant_engine/features/loader.py apps/run_realtime.py tests/unit/quant_engine/health`
   - Result: `All checks passed`

2. `pytest -q tests/unit/quant_engine/health tests/unit/test_step_trace_logging.py tests/integration/runtime/test_loader_wiring_smoke.py tests/integration/runtime/test_realtime_wiring_smoke.py tests/runtime/test_runtime_wiring_contracts.py tests/runtime/test_runtime_source_isolation.py tests/system/test_readiness_gate.py`
   - Result: `34 passed`
   - Warnings: existing pandas deprecation warnings in ingestion source path (non-blocking).

## 6. Files Changed

Modified:
- `apps/run_realtime.py`
- `src/quant_engine/features/extractor.py`
- `src/quant_engine/features/loader.py`
- `src/quant_engine/runtime/driver.py`
- `src/quant_engine/runtime/realtime.py`
- `src/quant_engine/runtime/snapshot.py`
- `src/quant_engine/strategy/engine.py`
- `src/quant_engine/strategy/loader.py`
- `src/quant_engine/utils/asyncio.py`

Added:
- `src/quant_engine/health/__init__.py`
- `src/quant_engine/health/config.py`
- `src/quant_engine/health/events.py`
- `src/quant_engine/health/manager.py`
- `src/quant_engine/health/policy.py`
- `src/quant_engine/health/restart.py`
- `src/quant_engine/health/session.py`
- `src/quant_engine/health/snapshot.py`
- `src/quant_engine/health/state.py`
- `tests/unit/quant_engine/health/test_action_coverage.py`
- `tests/unit/quant_engine/health/test_manager.py`
- `tests/unit/quant_engine/health/test_policy.py`
- `tests/unit/quant_engine/health/test_session.py`
- `tests/unit/quant_engine/health/test_state_machines.py`

## 7. Notes for Audit

- Health decision logic is centralized in `FaultPolicy.evaluate(...)`.
- Intercept sites apply returned actions without embedding additional policy.
- `ExecutionPermit` is implemented and enforced at step execution gate.
- Restart path is hooked through async task callbacks and `SourceRestartManager` scheduling.
- Session helper follows conservative default: uncertain calendar state => treat market as open (`market_is_closed -> False` on failure).
