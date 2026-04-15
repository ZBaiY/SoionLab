# Strategy Writer Roadmap

Internal handbook for implementing and evaluating strategies in the SoionLab repo.

## 1. Scope

This skill handles strategy work only.
It covers:
- family-based strategy placement
- focused strategy implementation
- focused strategy testing
- reusable research runner setup
- parameter scan and comparison workflow
- family-local research reporting

It does not justify broad engine redesign.

## 2. Placement

Reusable skill guidance belongs under:

```text
skills/Strategy_Writer/
  SKILL.md
  roadmap.md
  runner.md
  scripts/
    init_strategy_family.py
    strategy_research_layout.py
    run_strategy_research.py
    common_research.py
    templates/
  schemas/
  agents/
```

User-facing strategy code belongs under:

```text
apps/strategy/
  minimal_flip.py                       # public compatibility view
  strategies.py                         # public compatibility view
  RSI_ranges.py                         # public compatibility view
  rsi_adx_gateway.py                    # public compatibility view
  ...
  families/
    examples/
      __init__.py
      strategies.py
      minimal_flip.py
    rsi_adx_gateway/
      __init__.py
      gateway.py
      cooldown.py
      flush_dmi.py
      refined_entry.py
      uptrend_hold.py
      uptrend_hold_hard.py
      uptrend_hold_macd.py
    rsi_iv/
      __init__.py
      ranges.py
```

Layout rules:
- family packages under `apps/strategy/families/` are the source of truth
- top-level `apps/strategy/*.py` files are compatibility views when needed
- new work should land in the family package first
- do not create new flat top-level strategy files unless there is a compatibility reason

Focused tests belong under:

```text
tests/unit/strategies/
  test_<family_slug>.py
```

Family-local research artifacts belong under:

```text
docs/strategies/<family_slug>/
  run_research.py
  strategy_research_config.json
  research_results.json
  parameter_scan.csv
  <family_slug>_research_report.md
```

Layout rule:
- strategy-specific research scripts belong under `docs/strategies/<family_slug>/`
- generic reusable runner logic belongs under `skills/Strategy_Writer/scripts/`
- do not keep family-specific research code inside the generic skill package

## 3. Strategy System Grounding

The strategy system separates declaration from runtime execution:
- `apps/strategy/` declares what the strategy needs
- `src/quant_engine/` implements how the engine runs it

Relevant owner paths:
- `src/quant_engine/strategy/registry.py`
- `src/quant_engine/strategy/base.py`
- `src/quant_engine/strategy/loader.py`
- `src/quant_engine/strategy/engine.py`
- `src/quant_engine/utils/app_wiring.py`

Registry rule:
- strategy modules are auto-discovered from `apps.strategy`
- family packages are valid organization units
- top-level wrappers may exist for import compatibility

## 4. Standard Workflow

### 4.1 Discovery

Identify:
- the owning strategy family
- the closest reference strategy
- the actual decision owner
- the bound parameters that matter
- the feature dependencies already available
- the nearest focused tests
- the existing family docs under `docs/strategies/<family_slug>/`

Do not code before you know which family owns the behavior.

### 4.2 Family placement

Place the source-of-truth strategy code under:
- `apps/strategy/families/<family_slug>/`

Keep or add top-level wrappers only when:
- existing imports or tests depend on them
- app-facing public entry modules need stable names

### 4.3 Minimal implementation

Implement the smallest valid strategy-local change.

Rules:
- do not redesign runtime, execution, or portfolio architecture
- prefer local strategy-family changes over broad abstractions
- preserve no-lookahead behavior
- preserve timing semantics

### 4.4 Focused tests first

Write or update focused tests before the implementation when the task changes behavior.

Minimum expectations:
- strategy still standardizes correctly
- registration/import path still works
- intended entry/exit behavior is covered
- negative or protected cases are covered
- parameter bindings/defaults are covered

Default command:

```bash
PYTHONPATH=src pytest -q tests/unit/strategies/test_<family_slug>.py
```

### 4.5 Reusable research workflow

For family-local research:
- keep the family-local entrypoint under `docs/strategies/<family_slug>/run_research.py`
- use the generic runner or common helpers under `skills/Strategy_Writer/scripts/`
- store family-specific config in the family docs directory

Common workflow:
1. parameter scan on IS windows
2. choose best configuration by a simple, explicit objective
3. run OOS comparison against references
4. summarize trade-count effects and behavior changes
5. write JSON, CSV, and markdown artifacts

### 4.6 Report writing

Reports must answer:
- what changed
- which parameters were scanned
- which configuration won IS
- what held up OOS
- whether gains are robust
- whether changes are only trade-count effects
- recommendation status

## 5. Reusable Script Contracts

### `init_strategy_family.py`

Creates or updates:
- `apps/strategy/families/<family_slug>/__init__.py`
- `tests/unit/strategies/test_<family_slug>.py`
- `docs/strategies/<family_slug>/run_research.py`
- `docs/strategies/<family_slug>/strategy_research_config.json`

### `run_strategy_research.py`

Consumes one config file describing:
- primary strategy
- reference strategies
- bind symbols
- parameter grid
- IS windows
- OOS windows
- output directory

Produces:
- `research_results.json`
- `parameter_scan.csv`
- `research_report.md`

### `common_research.py`

Provides shared helpers for:
- backtest runs
- loading summary metrics
- scoring scan rows
- aggregate window summaries
- head-to-head comparisons

## 6. Validation Standard

A strategy task is valid only if:
- family placement is correct
- import compatibility is preserved where required
- focused tests are green
- strategy-specific research logic is not embedded in the generic skill package
- family-local docs contain the research outputs when research work was requested
