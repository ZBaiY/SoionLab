<!-- PR checklist
- Checked: app -> registry -> standardize -> loader -> engine flow and ingestion init ownership.
- Fixed: loader model_cfg guard, app builders, import-time logging side effects, dead loader code.
- Left unchanged: runtime engine semantics, handler lifecycle, and EXAMPLE unused handlers (intentional).
-->
# Loader Wiring Audit

## Scope and invariants checked
- Strategy is static spec only; loader assembles engine without runtime/time semantics.
- Loader calls `strategy.standardize(overrides)` before wiring components.
- Driver/apps own ingestion init; loader does not create sources/workers.
- App entrypoints resolve strategy via registry and pass overrides only to standardize.

## Config flow diagram
```
apps/run_*.py
  -> get_strategy(name)
  -> StrategyBase.bind(...)
  -> StrategyLoader.from_config(strategy, mode, overrides)
       -> strategy.standardize(overrides)
       -> assemble handlers/features/models/decision/risk/execution/portfolio
  -> engine
```

## Ingestion initialization locations
- Backtest: `apps/run_backtest.py:build_backtest_engine` builds plan via `_build_backtest_ingestion_plan`; tasks start in `apps/run_backtest.py:main`.
- Realtime: `apps/run_realtime.py:build_realtime_engine` builds plan via `_build_realtime_ingestion_plan`; tasks start in `apps/run_realtime.py:main`.
- Mock: no ingestion; `apps/run_mock.py:build_mock_engine` returns empty plan.

## Observed wiring issues or ambiguities
- `src/quant_engine/strategy/loader.py` accessed `model_cfg.get(...)` even when `MODEL_CFG` is `None`, breaking model-less strategies like RSI_ADX.
- `apps/run_backtest.py` and `apps/run_realtime.py` built engines without reusable builders, making wiring untestable and mixing app init with runtime loops.
- `src/quant_engine/models/loader.py` and `src/quant_engine/strategy/saved_strategies/example.py` were unreferenced legacy wiring paths.

## Fixes applied
- Added builder functions for app wiring and isolated ingestion plans: `apps/run_backtest.py`, `apps/run_realtime.py`, `apps/run_mock.py`.
- Guarded model-less strategies in loader and reused standardized params: `src/quant_engine/strategy/loader.py`.

## Dead-code removal summary
- `src/quant_engine/models/loader.py` (unused model loader wrapper; superseded by registry).
- `src/quant_engine/strategy/symbol_discovery.py` (unused legacy helper).
- `src/quant_engine/strategy/saved_strategies/example.py` and saved_strategies import in `src/quant_engine/strategy/registry.py` (unreferenced legacy strategy).
- `build_strategy` in `src/quant_engine/strategy/registry.py` (unused factory; registry access uses `get_strategy`).

## Guards/tests added
- Loader smoke: `tests/integration/runtime/test_loader_wiring_smoke.py`.
- App builder wiring: `tests/integration/runtime/test_app_builders.py`.

## Remaining risks / intentional deviations
- Realtime sources in `apps/run_realtime.py` remain placeholders (stream source without live wiring); expected for local dev.
- EXAMPLE strategy includes unused handlers intentionally to validate robustness; left unchanged.

## Final verdict
Loader wiring now satisfies the stated architecture: apps own ingestion init, loader only assembles engine from standardized strategy config, and registry resolution is explicit and testable.
