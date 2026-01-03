

# Strategy / Spec Naming & Wiring Conventions (Quant_Engine v4)

This document records the **implicit rules already used in the codebase**.  
Nothing here is aspirational; these are the conventions the loader, standardizer, and registries already assume.

---

## 1. Two namespaces: *registry identifiers* vs *runtime names*

There are **two different naming layers** in the system. Mixing them is the root cause of most confusion.

### 1.1 Registry identifiers (types)

Used by registries and configs:

- `@register_strategy("RSI-ADX-SIDEWAYS")`
- `@register_feature("RSI-MEAN")`
- `@register_decision("RSI-DYNAMIC-BAND")`
- `@register_model("PAIR-ZSCORE")`
- `@register_risk("FULL-ALLOCATION")`

**Rules**
- Use **kebab-case** (`-`) exclusively.
- These strings are *identifiers*, not semantic names.
- They are never parsed structurally.

Examples:

```
RSI
RSI-MEAN
RSI-STD
RSI-DYNAMIC-BAND
ATR-SIZER
FULL-ALLOCATION
```

---

### 1.2 Runtime feature names (identity)

Used inside the engine at runtime, passed between layers.

**Canonical form**

```
TYPE_PURPOSE_SYMBOL[^REF]
```

Examples:

```
RSI_DECISION_BTCUSDT
RSI_MEAN_DECISION_BTCUSDT
SPREAD_MODEL_BTCUSDT^ETHUSDT
ZSCORE_MODEL_BTCUSDT^ETHUSDT
ATR_RISK_BTCUSDT
```

**Rules**

- Exactly **two `_`** in the base name.
- Optional `^REF` suffix only for pair / ref features.
- `_` is reserved **only** for canonical feature names.
- `-` is **never** used inside canonical feature names.

> Registry names may contain `-`; runtime feature names may not.

---

## 2. Feature naming is not free-form

In `FEATURES_USER`, the `name` field is **semantic**, not decorative.

During `Strategy.standardize()`:

- `type` → `TYPE`
- inferred or explicit `purpose` → `PURPOSE`
- `symbol` → `SYMBOL`
- `params.ref` → `^REF`

The loader **rebuilds** the canonical name.

This means:

- Writing `"RSI-MEAN_DECISION_{A}"` is allowed as *input*.
- The standardized output will always follow `TYPE_PURPOSE_SYMBOL[^REF]`.

---

## 3. Placeholders and binding (`{}`)

### 3.1 Where placeholders are allowed

Placeholders like `{A}`, `{B}`, `{window_RSI}` may appear in:

- `UNIVERSE_TEMPLATE`
- `DATA`
- `FEATURES_USER`
- `MODEL_CFG`, `DECISION_CFG`, `RISK_CFG`, `EXECUTION_CFG`, `PORTFOLIO_CFG`

They must **not** appear after binding.

---

### 3.2 Binding is structural, not runtime

```
strategy_tpl = RSIADXSidewaysStrategy()
strategy = strategy_tpl.bind(
    A="BTCUSDT",
    window_RSI=14,
    window_ADX=14,
    window_RSI_rolling=23,
)
```

**Rules**

- `bind()` only resolves templates.
- No timestamps, warmup, history, or mode enters here.
- Binding produces a *new immutable strategy spec*.

---

## 4. Multi-symbol semantics: `primary` and `ref`

### 4.1 Universe

```
UNIVERSE_TEMPLATE = {
    "primary": "{A}",
    "secondary": {"{B}"},
}
```

- `primary` is the execution symbol.
- `secondary` symbols are only data dependencies.

---

### 4.2 Pair features / models

Use **`ref`**, never `secondary`:

```
{
  "type": "SPREAD",
  "symbol": "{A}",
  "params": {"ref": "{B}"}
}
```

Canonical naming automatically becomes:

```
SPREAD_MODEL_A^B
```

---

## 5. Strategy class fields (spec only)

A strategy class is a **static DSL**, not a runtime object.

Allowed class-level fields:

```
DATA
REQUIRED_DATA
FEATURES_USER
MODEL_CFG
DECISION_CFG
RISK_CFG
EXECUTION_CFG
PORTFOLIO_CFG
PRESETS
```

Forbidden:

- start / end timestamps
- warmup steps
- handlers
- engine mode

---

## 6. Dataclass rule (important)

**Strategy specs must NOT be dataclasses.**

Why:

- `@dataclass` injects an `__init__` that overwrites class attributes.
- This silently destroys registry-based strategies.

Correct split:

- **Strategy class**: pure class attributes (DSL)
- **Normalized config / EngineSpec**: dataclass

---

## 7. Interval semantics

- `Strategy.INTERVAL` defines observation frequency.
- Data blocks may define their own `interval`.
- `standardize()` converts intervals to `interval_ms`.

Never encode warmup or history length into the strategy interval.

---

## 8. Loader / Engine expectations

- Strategy → declaration only
- Loader → wiring + instantiation
- Engine → runtime semantics and step ordering

No layer may leak responsibilities upward or downward.

---

## 9. Quick mental checklist

If something breaks, check:

1. Did I mix `-` and `_`?
2. Did a placeholder survive past `bind()`?
3. Did I accidentally make a Strategy a dataclass?
4. Did I use `secondary` instead of `ref`?
5. Does the feature name still match `TYPE_PURPOSE_SYMBOL[^REF]`?

If all five are correct, wiring usually is too.