# Strategy Authoring Roadmap

Internal developer handbook for implementing new strategies in the SoionLab quant engine.

---

## 1. Strategy System Overview

The system separates strategy **declaration** from strategy **execution** across two layers.

### User Strategy Layer (`apps/strategy/`)

This is where strategy authors work. Each strategy is a Python class that **declares** what it needs: data domains, features, model type, decision logic, risk rules, execution policy, and portfolio type. Strategy classes live in `apps/strategy/strategies.py` (or any `.py` module under `apps/strategy/`).

A strategy author never writes execution loops, data handling, or feature computation code here. The class is purely declarative.

### Core Engine Layer (`src/quant_engine/`)

The engine consumes a strategy declaration and wires together all runtime components: data handlers, feature extractors, models, decision modules, risk rules, execution engines, and portfolio managers. The engine orchestrates the step-by-step pipeline at runtime.

**Key principle**: `apps/` declares *what*; `src/` implements *how*.

### Entry Points

| Entry Point | File | Purpose |
|---|---|---|
| Backtest | `apps/run_backtest.py` | CLI entry; selects strategy by name, binds symbols, runs via `BacktestDriver` |
| Realtime | `apps/run_realtime.py` | Live trading entry point |
| Mock | `apps/run_mock.py` | Mock/paper trading |

All entry points call `build_backtest_engine()` (in `src/quant_engine/utils/app_wiring.py`) or equivalent builders that resolve the strategy name from the registry, standardize the config, and construct a `StrategyEngine`.

---

## 2. Where a New Strategy Should Be Placed

```
apps/
  strategy/
    __init__.py          # package marker ("User strategy package.")
    strategies.py        # <-- EXAMPLES YOU CAN REFER TO/PUT YOUR STRATEGY HERE
    xx.py                # <-- PUT YOUR STRATEGY HERE
```

Any `.py` module inside `apps/strategy/` is auto-discovered and imported by the strategy registry. The discovery mechanism is in `src/quant_engine/strategy/registry.py`:

```python
# registry.py — load_strategy_modules()
spec = importlib.util.find_spec("apps.strategy")
for module_info in pkgutil.iter_modules(spec.submodule_search_locations):
    importlib.import_module(f"apps.strategy.{module_info.name}")
```

This means:
- You can add new files like `apps/strategy/my_new_strategy.py` and they will be auto-discovered.
- Or you can add classes to the existing `apps/strategy/strategies.py`.
- No manual import or registration list is needed beyond the `@register_strategy` decorator.

**What does NOT belong in `apps/strategy/`**:
- Feature computation logic (belongs in `src/quant_engine/features/`)
- Model implementations (belongs in `src/quant_engine/models/`)
- Decision modules (belongs in `src/quant_engine/decision/`)
- Risk rules (belongs in `src/quant_engine/risk/`)
- Execution policies (belongs in `src/quant_engine/execution/`)

There is also `src/quant_engine/strategy/strategies.py` which contains internal/debug-only strategy definitions (suffixed `-DEBUG`). User-facing strategies go under `apps/strategy/`.

---

## 3. Strategy Lifecycle Inside the Engine

The full path from strategy declaration to execution:

```
apps/strategy/strategies.py     (1) Strategy class declared
        │
        ▼
registry.get_strategy(name)     (2) Registry resolves class by name
        │
        ▼
StrategyCls.standardize(        (3) Bind symbols + normalize config
  symbols={...}                      → NormalizedStrategyCfg
)
        │
        ▼
StrategyLoader.from_config()    (4) Build all runtime components
        │
        ▼
StrategyEngine                  (5) Orchestrates step() loop
  │
  ├── handlers  → market data snapshots
  ├── features  → feature_extractor.update(ts)
  ├── models    → model.predict(features)
  ├── decision  → decision.decide(context) → score
  ├── risk      → risk.adjust(score, context) → target_position
  ├── execution → execution.execute(target, ...) → fills
  └── portfolio → portfolio.apply_fill(fill)
```

### Stage-by-stage breakdown

#### Stage 1: Strategy Registration
**Module**: `src/quant_engine/strategy/registry.py`
**Class**: `STRATEGY_REGISTRY` (global dict)
**Mechanism**: `@register_strategy("NAME")` decorator

When `get_strategy("NAME")` is called, it triggers `load_strategy_modules()` which imports `quant_engine.strategy.strategies` (internal) and all modules under `apps.strategy.*` (user). Each decorated class gets registered in the global `STRATEGY_REGISTRY` dict.

#### Stage 2: Symbol Binding & Standardization
**Module**: `src/quant_engine/strategy/base.py`
**Class**: `StrategyBase`
**Method**: `standardize(overrides, symbols={"A": "BTCUSDT", ...})`
**Returns**: `NormalizedStrategyCfg` (frozen dataclass in `src/quant_engine/strategy/config.py`)

This stage:
- Resolves `{A}`, `{B}`, etc. placeholders in all config fields
- Expands `$ref` preset references (e.g., `"$ref": "OHLCV_15M_180D"` → full handler config)
- Normalizes feature names to canonical format: `TYPE_PURPOSE_SYMBOL[^REF]`
- Validates data domain coverage (`REQUIRED_DATA` vs `DATA` keys)
- Converts interval strings to epoch milliseconds

Presets are resolved in order: global presets (`src/quant_engine/strategy/presets/`) → strategy-level `PRESETS` → runtime overrides.

#### Stage 3: Component Construction (Loader)
**Module**: `src/quant_engine/strategy/loader.py`
**Class**: `StrategyLoader`
**Method**: `from_config(strategy, mode, overrides)`

The loader reads the normalized config and constructs every runtime component via subsystem loaders:

| Component | Loader | Registry | Config Key |
|---|---|---|---|
| Data Handlers | `build_multi_symbol_handlers()` in `data/builder.py` | — | `data` |
| Features | `FeatureLoader.from_config()` in `features/loader.py` | `features/registry.py` | `features_user` |
| Model | `build_model()` in `models/registry.py` | `@register_model` | `model` |
| Decision | `DecisionLoader.from_config()` in `decision/loader.py` | `@register_decision` | `decision` |
| Risk | `RiskLoader.from_config()` in `risk/loader.py` | `@register_risk` | `risk.rules` |
| Execution | `ExecutionLoader.from_config()` in `execution/loader.py` | 4 sub-registries | `execution` |
| Portfolio | `PortfolioLoader.from_config()` in `portfolio/loader.py` | `@register_portfolio` | `portfolio` |

After construction, the loader performs cross-validation:
- Ensures all feature names referenced by model/risk/decision exist
- Binds feature name indices into model/risk/decision via `bind_feature_index()`
- Validates feature type coverage

#### Stage 4: Runtime Execution (Engine Step)
**Module**: `src/quant_engine/strategy/engine.py`
**Class**: `StrategyEngine`
**Method**: `step(ts=timestamp)`

The step method enforces a strict ordering invariant:

```
STEP_ORDER = (
    "handlers",   → collect market snapshots from all data handlers
    "features",   → feature_extractor.update(timestamp) → dict[str, Any]
    "models",     → model.predict_with_context(features, context) → float
    "decision",   → decision.decide(context) → float (score)
    "risk",       → risk_manager.adjust(score, context) → float (target_position)
    "execution",  → execution_engine.execute(target, portfolio, snapshots) → fills
    "portfolio",  → portfolio.apply_fill(fill) for each fill
    "snapshot",   → build immutable EngineSnapshot
)
```

The `context` dict passed to decision contains:
```python
{
    "timestamp": int,           # epoch ms
    "features": dict,           # all computed feature values
    "models": dict,             # model output scores
    "portfolio": dict,          # portfolio state snapshot
    "primary_snapshots": dict,  # primary symbol market data by domain
    "market_snapshots": dict,   # all symbols market data by domain
    "readiness_ctx": dict,      # soft readiness status
}
```

#### Stage 5: App Wiring (Backtest)
**Module**: `src/quant_engine/utils/app_wiring.py`
**Function**: `build_backtest_engine(strategy_name, bind_symbols, start_ts, end_ts, data_root)`

This function:
1. Calls `get_strategy(name)` to retrieve the class
2. Calls `standardize(symbols=bind_symbols)` to normalize
3. Injects `data_root` into handler configs
4. Calls `StrategyLoader.from_config()` to build the engine
5. Builds an ingestion plan (file-source workers per domain/symbol)

Returns `(engine, driver_cfg, ingestion_plan)`.

The `BacktestDriver` (`src/quant_engine/runtime/backtest.py`) then runs the time loop, feeding ticks from an `asyncio.PriorityQueue` into the engine's handlers and calling `engine.step(ts=...)` at each observation interval.

---

## 4. Minimal Steps to Add a Strategy

### Step 1: Create the strategy class

Create or edit `apps/strategy/strategies.py` (or a new `.py` file under `apps/strategy/`):

```python
from quant_engine.strategy.base import StrategyBase
from quant_engine.strategy.registry import register_strategy

@register_strategy("MY-STRATEGY")
class MyStrategy(StrategyBase):
    STRATEGY_NAME = "MY-STRATEGY"
    INTERVAL = "15m"                      # observation interval
    ...
```

### Step 2: Define the universe template

```python
    UNIVERSE_TEMPLATE = {
        "primary": "{A}",
        # optional: "secondary": {"{B}"},
        "soft_readiness": {
            "enabled": False,
            "domains": ["orderbook", "option_chain", "iv_surface", "sentiment"],
            "max_staleness_ms": 300000,
        },
    }
```

`{A}`, `{B}`, etc. are placeholders resolved at runtime via `bind_symbols`. For a single-symbol strategy, only `{A}` is needed.

### Step 3: Declare data requirements

```python
    REQUIRED_DATA = {"ohlcv"}           # domains that must be present

    DATA = {
        "primary": {
            "ohlcv": {"$ref": "OHLCV_15M_180D"},
        },
        # optional secondary symbols:
        # "secondary": {
        #     "{B}": {
        #         "ohlcv": {"$ref": "OHLCV_15M_180D"},
        #     }
        # },
    }
```

Available data presets (defined in `src/quant_engine/strategy/presets/`):

| Preset Name | Domain | Details |
|---|---|---|
| `OHLCV_1M_30D` | ohlcv | 1m interval, 30d lookback |
| `OHLCV_15M_180D` | ohlcv | 15m interval, 180d lookback |
| `OPTION_CHAIN_5M` | option_chain | 5m interval |
| `ORDERBOOK_L2_10_100MS` | orderbook | L2, depth 10 |
| `IV_SURFACE_5M` | iv_surface | 5m, SSVI calibrator |
| `SENTIMENT_BASIC_5M` | sentiment | 5m |

### Step 4: Declare features

```python
    FEATURES_USER = [
        {
            "name": "RSI_DECISION_{A}",    # canonical: TYPE_PURPOSE_SYMBOL
            "type": "RSI",                  # must match a @register_feature key
            "symbol": "{A}",
            "params": {"window": 14},
        },
    ]
```

**Feature naming convention** (enforced by `standardize()`):
```
TYPE_PURPOSE_SYMBOL
TYPE_PURPOSE_SYMBOL^REF
```
- `TYPE`: feature type (e.g., `RSI`, `ATR`, `SPREAD`, `ZSCORE`)
- `PURPOSE`: how this feature is consumed — `MODEL`, `DECISION`, or `RISK`
- `SYMBOL`: trading symbol
- `REF`: optional reference/secondary symbol (for pair features)

Available feature types (registered in `src/quant_engine/features/registry.py`):
- TA: `RSI`, `ADX`, `RSI-MEAN`, `RSI-STD`, `ATR`, and more (from `features/ta/ta.py`)
- Volatility: from `features/volatility/volatility.py`
- Microstructure: from `features/microstructure/microstructure.py`
- Options: `IV`, `IV-SURFACE` (from `features/options/`)
- Pair/ref features: `SPREAD`, `ZSCORE`, etc. (from `features/wth_ref/double.py`)

### Step 5: Configure model (optional)

```python
    MODEL_CFG = {
        "type": "PAIR-ZSCORE",            # must match a @register_model key
        "params": {
            "zscore_feature": "ZSCORE_MODEL_{A}^{B}",
            "secondary": "{B}",
        },
    }
```

Set `MODEL_CFG = None` for rule-based strategies that don't need a model (the engine skips model prediction).

Available models (registered in `src/quant_engine/models/registry.py`): implementations from `models/momentum.py`, `models/statistical.py`, `models/regime.py`, `models/ml_model.py`, `models/physics.py`.

### Step 6: Configure decision

```python
    DECISION_CFG = {
        "type": "RSI-DYNAMIC-BAND",       # must match a @register_decision key
        "params": {
            "rsi": "RSI_DECISION_{A}",
            "adx": "ADX_DECISION_{A}",
            "adx_threshold": 25,
            ...
        },
    }
```

The decision module's `decide(context)` receives the full context dict and must return a `float` score. Available decision types are registered in `src/quant_engine/decision/registry.py`, which imports from `threshold.py`, `regime.py`, `fusion.py`.

### Step 7: Configure risk rules

```python
    RISK_CFG = {
        "shortable": False,
        "rules": {
            "FULL-ALLOCATION": {},
            "CASH-POSITION-CONSTRAINT": {
                "params": {
                    "fee_rate": 0.001,
                    "slippage_bound_bps": 10,
                    "min_notional": 10.0,
                },
            },
        },
    }
```

Risk rules are applied sequentially by `RiskEngine`. Each rule's `adjust(score, context)` can modify the target position. Available rules (from `src/quant_engine/risk/registry.py`):
- `ATR-SIZER` (from `rules_atr.py`)
- `EXPOSURE-LIMIT` (from `rules_exposure.py`)
- `SENTIMENT-GATE` (from `rules_sentiment.py`)
- `STOP-LOSS` (from `rules_sl.py`)
- `TAKE-PROFIT` (from `rules_tp.py`)
- `FULL-ALLOCATION` (from `rule_full.py`)
- `CASH-POSITION-CONSTRAINT`, `FRACTIONAL-CASH-CONSTRAINT` (from `rules_constraints.py`)

### Step 8: Configure execution

```python
    EXECUTION_CFG = {
        "policy":   {"type": "IMMEDIATE"},   # IMMEDIATE | TWAP | MAKER-FIRST
        "router":   {"type": "SIMPLE"},      # SIMPLE | L1-AWARE
        "slippage": {"type": "LINEAR"},      # LINEAR | DEPTH
        "matching": {"type": "SIMULATED"},   # SIMULATED | LIVE-BINANCE
    }
```

Each execution sub-component has its own registry under `src/quant_engine/execution/`.

### Step 9: Configure portfolio

```python
    PORTFOLIO_CFG = {
        "type": "STANDARD",                  # STANDARD | FRACTIONAL
        "params": {"initial_capital": 1000000},
    }
```

- `STANDARD` (`portfolio/manager.py`): integer lot-based portfolio
- `FRACTIONAL` (`portfolio/fractional.py`): supports fractional quantities with configurable `step_size`

### Step 10: Run the strategy

From `apps/run_backtest.py`, set the strategy name and symbol bindings:

```python
STRATEGY_NAME = "MY-STRATEGY"
BIND_SYMBOLS = {"A": "BTCUSDT"}
```

Or use CLI arguments:

```bash
python -m apps.run_backtest \
    --strategy MY-STRATEGY \
    --symbols "A=BTCUSDT" \
    --start-ts 1700000000000 \
    --end-ts 1700100000000
```

---

## 5. Strategy Design Guidelines

### What belongs in strategy vs feature

| Concern | Where |
|---|---|
| Which features to compute | Strategy (`FEATURES_USER`) |
| How to compute a feature value | Feature channel (`src/quant_engine/features/`) |
| Which data domains are needed | Strategy (`DATA`, `REQUIRED_DATA`) |
| How data is cached/windowed | Data handler (`src/quant_engine/data/`) |

A strategy **declares** feature dependencies. It never implements feature computation logic. If you need a new indicator not yet in the feature registry, implement it as a `FeatureChannel` subclass in `src/quant_engine/features/`, register it with `@register_feature("MY_INDICATOR")`, then reference it by type in your strategy's `FEATURES_USER`.

### What belongs in model vs strategy

- **Strategy**: selects which model type to use and passes parameters via `MODEL_CFG`
- **Model**: implements `predict(features) → float` and declares `required_feature_types`

The strategy does not call the model directly. The engine calls it during the step loop.

### What belongs in decision vs model

- **Model**: produces a predictive score from features (e.g., z-score, momentum signal)
- **Decision**: converts model score + features + portfolio state into a directional signal (`float`)

Some strategies are rule-based (no model). In that case, set `MODEL_CFG = None` and implement all logic in the decision module. The decision receives `context["models"]` which will be empty.

### Feature naming rules (enforced)

The engine's `standardize()` method rewrites all feature names to the canonical form `TYPE_PURPOSE_SYMBOL[^REF]`. The `PURPOSE` field is inferred from the second segment of the user-provided name. This purpose tag controls which component receives the feature:

- `_MODEL_` features → bound to the model via `set_required_features()`
- `_DECISION_` features → bound to the decision module
- `_RISK_` features → bound to risk rules

If you name a feature `ATR_RISK_BTCUSDT`, the engine will automatically bind it for risk rule consumption. Name your features with the correct purpose segment.

### How to avoid breaking engine assumptions

1. **Do not add runtime keys to strategy config.** `start_ts`, `end_ts`, `warmup_steps`, `warmup_to`, `warmup` are forbidden in `standardize()` — they belong to the driver, not the strategy.

2. **All symbols referenced in features/model must be declared in `DATA`.** The loader validates that every symbol in `FEATURES_USER[*].symbol`, `FEATURES_USER[*].params.ref`, and `MODEL_CFG.params.ref`/`secondary` exists in either `DATA.primary` (as the primary symbol) or `DATA.secondary`.

3. **Feature names must be unique.** Duplicate feature names in `FEATURES_USER` cause a `ValueError` at load time.

4. **`REQUIRED_DATA` must be non-empty and must be a subset of domains declared in `DATA`.** The `validate_spec()` method enforces this.

5. **Step ordering is strict.** The engine enforces `handlers → features → models → decision → risk → execution → portfolio → snapshot`. Components cannot call each other out of order.

6. **Timestamps are epoch milliseconds (int).** All `context["timestamp"]` values, data handler timestamps, and portfolio timestamps are epoch ms. Never pass seconds or datetime objects.

7. **Strategy classes are stateless declarations.** The `StrategyBase` subclass stores only class-level config attributes. Runtime state lives in the engine and its components.

---

## 6. Example Strategy Skeletons

Both examples below are from the existing `apps/strategy/strategies.py`.

### Example A: Pair Trading Strategy (Model-Based)

```python
@register_strategy("EXAMPLE")
class ExampleStrategy(StrategyBase):

    STRATEGY_NAME = "EXAMPLE"
    INTERVAL = "30m"

    UNIVERSE_TEMPLATE = {
        "primary": "{A}",
        "secondary": {"{B}"},
        "soft_readiness": {
            "enabled": False,
            "domains": ["orderbook", "option_chain", "iv_surface", "sentiment"],
            "max_staleness_ms": 300000,
        },
    }

    DATA = {
        "primary": {
            "ohlcv": {"$ref": "OHLCV_15M_180D"},
            "option_chain": {"$ref": "OPTION_CHAIN_5M"},
        },
        "secondary": {
            "{B}": {
                "ohlcv": {"$ref": "OHLCV_15M_180D"},
            }
        },
    }

    REQUIRED_DATA = {"ohlcv", "option_chain"}

    FEATURES_USER = [
        {
            "name": "SPREAD_MODEL_{A}^{B}",
            "type": "SPREAD",
            "symbol": "{A}",
            "params": {"ref": "{B}"},
        },
        {
            "name": "ZSCORE_MODEL_{A}^{B}",
            "type": "ZSCORE",
            "symbol": "{A}",
            "params": {"ref": "{B}", "lookback": 120},
        },
        {
            "name": "ATR_RISK_{A}",
            "type": "ATR",
            "symbol": "{A}",
            "params": {"window": 14},
        },
    ]

    MODEL_CFG = {
        "type": "PAIR-ZSCORE",
        "params": {"zscore_feature": "ZSCORE_MODEL_{A}^{B}", "secondary": "{B}"},
    }

    DECISION_CFG = {
        "type": "ZSCORE-THRESHOLD",
        "params": {
            "zscore_feature": "ZSCORE_MODEL_{A}^{B}",
            "enter": 2.0,
            "exit": 0.5,
        },
    }

    RISK_CFG = {
        "shortable": False,
        "rules": {
            "ATR-SIZER": {"params": {"atr_feature": "ATR_RISK_{A}"}},
            "EXPOSURE-LIMIT": {"params": {"limit": 2.0}},
            "CASH-POSITION-CONSTRAINT": {
                "params": {
                    "fee_rate": 0.001,
                    "slippage_bound_bps": 10,
                    "min_notional": 10.0,
                },
            },
        },
    }

    EXECUTION_CFG = {
        "policy": {"type": "IMMEDIATE"},
        "router": {"type": "SIMPLE"},
        "slippage": {"type": "LINEAR"},
        "matching": {"type": "SIMULATED"},
    }

    PORTFOLIO_CFG = {
        "type": "STANDARD",
        "params": {"initial_capital": 1000000},
    }
```

**Key characteristics**: uses two symbols (`{A}` primary, `{B}` secondary), a `PAIR-ZSCORE` model, z-score threshold decision, and ATR-based risk sizing.

Run with: `--strategy EXAMPLE --symbols "A=BTCUSDT,B=ETHUSDT"`

### Example B: Single-Symbol Rule-Based Strategy (No Model)

```python
@register_strategy("RSI-ADX-SIDEWAYS")
class RSIADXSidewaysStrategy(StrategyBase):

    STRATEGY_NAME = "RSI-ADX-SIDEWAYS"
    INTERVAL = "15m"

    UNIVERSE_TEMPLATE = {
        "primary": "{A}",
        "soft_readiness": {
            "enabled": False,
            "domains": ["orderbook", "option_chain", "iv_surface", "sentiment"],
            "max_staleness_ms": 300000,
        },
    }

    DATA = {
        "primary": {
            "ohlcv": {"$ref": "OHLCV_15M_180D"},
        }
    }

    REQUIRED_DATA = {"ohlcv"}

    FEATURES_USER = [
        {"name": "RSI_DECISION_{A}",      "type": "RSI",      "symbol": "{A}", "params": {"window": '{window_RSI}'}},
        {"name": "ADX_DECISION_{A}",      "type": "ADX",      "symbol": "{A}", "params": {"window": '{window_ADX}'}},
        {"name": "RSI-MEAN_DECISION_{A}", "type": "RSI-MEAN", "symbol": "{A}", "params": {"window_rsi": "{window_RSI}", "window_rolling": "{window_RSI_rolling}"}},
        {"name": "RSI-STD_DECISION_{A}",  "type": "RSI-STD",  "symbol": "{A}", "params": {"window_rsi": "{window_RSI}", "window_rolling": "{window_RSI_rolling}"}},
    ]

    MODEL_CFG = None  # rule-based: no model

    DECISION_CFG = {
        "type": "RSI-DYNAMIC-BAND",
        "params": {
            "rsi": "RSI_DECISION_{A}",
            "rsi_mean": "RSI-MEAN_DECISION_{A}",
            "rsi_std": "RSI-STD_DECISION_{A}",
            "adx": "ADX_DECISION_{A}",
            "adx_threshold": 25,
            "variance_factor": 1.8,
            "mae": 0.0,
        },
    }

    RISK_CFG = {
        "shortable": False,
        "rules": {
            "FULL-ALLOCATION": {},
            "CASH-POSITION-CONSTRAINT": {
                "params": {
                    "fee_rate": 0.001,
                    "slippage_bound_bps": 10,
                    "min_notional": 10.0,
                },
            },
        },
    }

    EXECUTION_CFG = {
        "policy": {"type": "IMMEDIATE"},
        "router": {"type": "SIMPLE"},
        "slippage": {"type": "LINEAR"},
        "matching": {"type": "SIMULATED"},
    }

    PORTFOLIO_CFG = {
        "type": "STANDARD",
        "params": {"initial_capital": 1000000},
    }
```

**Key characteristics**: single symbol, no model, RSI+ADX dynamic band decision, parameterized windows via `{window_RSI}` etc.

Run with: `--strategy RSI-ADX-SIDEWAYS --symbols "A=BTCUSDT,window_RSI=14,window_ADX=14,window_RSI_rolling=5"`

Note: `bind_symbols` is not limited to asset symbols — any `{placeholder}` in the strategy config can be bound, including numeric parameters like `window_RSI`.

---

## Appendix: Module Reference

| Path | Role |
|---|---|
| `src/quant_engine/strategy/base.py` | `StrategyBase` class — all strategies inherit from this |
| `src/quant_engine/strategy/registry.py` | `@register_strategy`, `get_strategy()`, auto-discovery |
| `src/quant_engine/strategy/config.py` | `NormalizedStrategyCfg` frozen dataclass |
| `src/quant_engine/strategy/loader.py` | `StrategyLoader.from_config()` — wires everything |
| `src/quant_engine/strategy/engine.py` | `StrategyEngine` — runtime step loop |
| `src/quant_engine/strategy/feature_resolver.py` | Feature config normalization and validation |
| `src/quant_engine/strategy/presets/` | Global data presets (`OHLCV_15M_180D`, etc.) |
| `src/quant_engine/features/registry.py` | `@register_feature`, `build_feature()` |
| `src/quant_engine/models/registry.py` | `@register_model`, `build_model()` |
| `src/quant_engine/decision/registry.py` | `@register_decision`, `build_decision()` |
| `src/quant_engine/risk/registry.py` | `@register_risk`, `build_risk()` |
| `src/quant_engine/risk/loader.py` | `RiskLoader` — builds `RiskEngine` from rule configs |
| `src/quant_engine/portfolio/registry.py` | `@register_portfolio`, `build_portfolio()` |
| `src/quant_engine/execution/loader.py` | `ExecutionLoader` — builds 4-component execution pipeline |
| `src/quant_engine/utils/app_wiring.py` | `build_backtest_engine()` — top-level backtest builder |
| `src/quant_engine/contracts/` | Protocol/base classes for all components |
