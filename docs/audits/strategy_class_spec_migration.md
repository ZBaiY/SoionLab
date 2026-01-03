# Strategy Class-Spec Migration

## Why change
`StrategyBase` was a dataclass carrying spec fields as instance attributes. Instantiating a strategy overwrote the declarative class attributes (the class-spec DSL), which made binding/standardization order fragile and caused subtle overrides when a strategy was created at runtime.

## New flow
Strategy specifications are now class-level only; instantiation is no longer part of the loader/app path.

```
StrategyCls
  -> bind_spec(symbols=...)
  -> standardize(overrides=..., symbols=...)
  -> NormalizedStrategyCfg
  -> StrategyLoader
```

`NormalizedStrategyCfg` is the frozen, typed output that the loader consumes. Use `cfg.to_dict()` only when a plain dict is required.

## Backward-compat notes
- Instance APIs remain as thin wrappers for tests/local scripts:
  - `StrategyCls().bind(...)` stores bind symbols and delegates to class-based standardization in the loader.
  - `strategy.build(...)` continues to work for bound instances.
- `StrategyBase.from_dict(...)` now builds a one-off strategy class with class-level spec fields; JSON-driven config is deprecated.
- App entrypoints and loader paths no longer instantiate strategies. Use `StrategyCls.standardize(..., symbols=...)` instead.
