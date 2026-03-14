---
name: strategy-writer
description: Use this skill when implementing, tuning, comparing, or reporting on trading strategies in this repository. Covers the standard SoionLab strategy workflow: inspect existing strategy families, add or refine a strategy locally, preserve old strategies, run focused tests, build a research script under docs/strategies/<slug>/, perform parameter scans, compare against reference strategies, analyze regimes and behavior changes, run in-sample and out-of-sample evaluations, summarize robustness, and produce sober research reports with reproducible artifacts.
---

# Strategy Writer

Use this skill for generic strategy work in this repo:
- adding a new strategy or decision
- refining an existing strategy locally
- running parameter scans
- comparing strategy variants
- doing IS/OOS evaluation
- analyzing regime behavior, trade frequency, drawdowns, and behavioral differences
- producing strategy research docs and reproducible outputs

Assume the user will usually provide a natural-language strategy brief first.
Your first job is to translate that brief into:
- exact strategy scope
- closest reference strategy family
- concrete parameter bindings and defaults
- intended entry/exit behavior
- focused tests
- research/evaluation outputs

## Working Directory

- Always work from the repo root:
  - `/Users/zhaoyub/Documents/Tradings/SoionLab`
- Run Python and pytest commands with:
  - `PYTHONPATH=src`
- Treat `apps/strategy/` as the user strategy area.
- Treat `docs/strategies/<strategy_slug>/` as the research workspace for one strategy family.

## Read First

Before editing code, inspect these:
- `skills/Strategy_Writer/roadmap.md`
- `skills/Strategy_Writer/writer.md`
- `skills/Strategy_Writer/OHLCV_GAP_HANDLING.md` for backtest-only OHLCV gap semantics
- the closest existing strategy under `apps/strategy/`
- the matching decision/model/risk/execution path under `src/quant_engine/`

Use `roadmap.md` for architecture and placement rules.
Use `writer.md` when you need a compact prompt/checklist for a concrete strategy task.
Reusable research runners and helpers belong under:
- `skills/Strategy_Writer/scripts/`

Strategy-local docs may keep thin wrapper scripts under `docs/strategies/<strategy_slug>/` that delegate into the skill script path.

Generic skill-side helpers available:
- `skills/Strategy_Writer/writer.md`
  - copy-paste prompt for full strategy implementation + research tasks
- `skills/Strategy_Writer/scripts/strategy_research_layout.py`
  - prints the recommended repo layout for a strategy family
- `skills/Strategy_Writer/scripts/templates/research_runner_template.py`
  - starter template for a new strategy-family research runner

## Naming System

Use consistent names across code, runs, docs, and outputs.

### Strategy and decision names

- Strategy registry names:
  - uppercase, hyphenated, repo-style
  - examples: `RSI-ADX-SIDEWAYS-FRACTIONAL`, `RSI-ADX-GATEWAY-FRACTIONAL`
- Decision registry names:
  - uppercase, hyphenated
  - example: `RSI-DYNAMIC-BAND-ADX-GATEWAY`
- Python module filenames:
  - lowercase snake or repo-local style under `apps/strategy/`
  - example: `apps/strategy/rsi_adx_gateway.py`

### Research folder names

- One family per folder:
  - `docs/strategies/<strategy_slug>/`
- Use a short lowercase slug.
- Keep all family research artifacts together there.

### Standard research files

Prefer this file set under `docs/strategies/<strategy_slug>/`:
- `run_research.py`
- `research_results.json`
- `parameter_scan.csv`
- `<strategy_slug>_research_report.md`

If you compare multiple logic versions, add explicit suffixes:
- `research_results_v1_*.json`
- `research_results_<model_id>.json`
- `parameter_scan_v1_*.csv`
- `<strategy_slug>_research_report_<model_id>.md`

### Run IDs

Use run IDs that reveal purpose and configuration:
- IS scan:
  - `IS_<FAMILY>_A30_V1p8_20240201`
- OOS comparison:
  - `OOS_<FAMILY>_TUNED_A30_V1p8_20240201`
- MAE or local sensitivity:
  - `IS_<FAMILY>_MAE_M0p25_20240201`
- full-window checks:
  - `BT_<FAMILY>_BEST_20241101_20260201`
- showcase or stitched reports:
  - `SHOWCASE_<FAMILY>_MULTIYEAR_AGGREGATE`

Keep parameter encoding stable:
- `1.8 -> 1p8`
- `0.25 -> 0p25`

## Standard Workflow

Follow this order unless the user explicitly narrows scope.

### 1. Discovery

Identify:
- the closest existing strategy family
- the decision class being mirrored
- the feature types already available
- the risk/execution/portfolio pattern already used
- the nearest tests
- the existing research docs for that family, if any

Do not code before you know:
- which files own the strategy declaration
- which object actually makes the trading decision
- where tunable parameters are bound
- what the current reference strategy is

### 2. Minimal implementation

Implement the smallest strategy-local change that satisfies the task.

Rules:
- do not modify old working strategies unless explicitly requested
- prefer local duplication over broad abstraction
- keep feature wiring minimal
- do not redesign runtime, execution, or portfolio architecture
- preserve no-lookahead behavior

Typical edit surface:
- `apps/strategy/<family>.py`
- nearest decision class or strategy-local feature wrapper
- nearest focused tests

### 3. Focused validation

Always add or update tests near the strategy family.

Minimum expectations:
- old strategy path remains untouched
- new/refined strategy standardizes correctly
- intended entries/exits work
- protected/non-trigger cases are covered
- any new parameter bindings are tested

Run targeted tests first:
- `PYTHONPATH=src pytest -q tests/unit/strategies/...`

### 4. Research script

If the task includes tuning or evaluation, create or update:
- `docs/strategies/<strategy_slug>/run_research.py`

Implementation note:
- Put the reusable script body under `skills/Strategy_Writer/scripts/<strategy_slug>/`
- Keep `docs/strategies/<strategy_slug>/run_research.py` as a thin wrapper when you need a stable docs-local entrypoint

The script should:
- be deterministic and reproducible
- run the backtest app through repo entrypoints
- reuse existing report infrastructure
- emit JSON, CSV, and markdown outputs
- compare the new strategy against one or more reference strategies

Prefer one script that produces:
- parameter scan
- IS aggregates
- OOS aggregates
- head-to-head comparison
- sensitivity checks
- final markdown report

### 5. Parameter search

Keep the search practical and interpretable.

Default rules:
- scan only the parameters directly tied to the strategy logic
- keep the grid modest
- document fixed parameters explicitly
- avoid giant hyperparameter sweeps unless the user asks

Common pattern:
- scan the decision thresholds and band-width parameters
- hold feature windows fixed first
- then do a small local sensitivity around the best point

### 6. IS / OOS split

Use repo-consistent rolling windows when possible.

Minimum standard:
- multiple IS windows for local ranking
- multiple OOS windows for validation
- head-to-head comparison against the old reference strategy

Do not recommend a strategy from IS alone.

### 7. Regime and behavior analysis

Always explain behavior, not just metrics.

At minimum analyze:
- when the strategy enters
- when it exits
- why trade frequency changes
- whether changes come from selectivity or new state transitions
- whether gains come with materially different turnover
- which regimes look good or bad

Useful behavior questions:
- does the change alter entry frequency or only exits?
- does it add churn?
- does it improve drawdown or only return?
- does it shift holding period?
- does it degrade in trend or sideways conditions?

### 8. Robustness checks

Do real robustness work, not a single best-run summary.

Default robustness set:
- rolling OOS windows
- reference-vs-new head-to-head
- local parameter sensitivity around the best point
- trade-count comparison
- note any invalid or excluded windows

If the repo already has a family-level robustness study, reuse it instead of creating a parallel framework.

### 9. Report writing

Write sober research reports under:
- `docs/strategies/<strategy_slug>/`

Report must answer:
- what changed behaviorally
- which parameters were scanned
- what won IS
- what held up OOS
- whether the effect looks robust
- whether gains are just trade-count changes
- recommendation status:
  - exploratory variant
  - challenger configuration
  - candidate for promotion

Do not oversell.

## Standard Commands

Use these patterns by default:

### Tests

```bash
PYTHONPATH=src pytest -q tests/unit/strategies/test_<family>.py
PYTHONPATH=src pytest -q tests/unit/test_strategy_standardize_snapshot.py
```

### Research run

```bash
PYTHONPATH=src python3 docs/strategies/<strategy_slug>/run_research.py
```

### Full-window backtest

```bash
PYTHONPATH=src python3 apps/run_backtest.py \
  --strategy <STRATEGY_NAME> \
  --symbols A=BTCUSDT,window_RSI=14,window_ADX=14,window_RSI_rolling=5 \
  --start-ts <START_MS> \
  --end-ts <END_MS> \
  --run-id <RUN_ID>
```

## Research Output Discipline

When you finish a strategy research task, leave behind:
- code changes
- focused tests
- one reproducible research script
- one JSON result file
- one CSV scan file
- one markdown report

If you compare multiple logic versions:
- preserve old results under explicit versioned filenames
- keep the default files pointing to the newest accepted model
- record the model ID in the output payload

## Guardrails

Do not:
- modify unrelated strategies
- mix stitched showcase metrics with continuous-run metrics without stating it
- compare unmatched configurations as if they were equivalent
- silently overwrite older research results without versioning when model logic changes
- report only the best local winner without OOS context

Prefer:
- narrow local code changes
- explicit reference strategy comparison
- regime-aware interpretation
- reproducible run IDs
- versioned research outputs when logic changes
