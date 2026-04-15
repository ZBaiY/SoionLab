---
name: OHLCV_feature_engineering
description: Use this skill when a user or agent needs to convert a natural-language OHLCV feature idea into a deterministic, auditable feature-research package for this repository. Covers direction parsing, bounded feature synthesis, leakage and timing audit, family classification, quality scoring, implementation handoff generation, and research-library writeback. Do not use it to implement feature code or strategy logic directly.
---

# OHLCV Feature Engineering

Use this skill for OHLCV feature research work in this repo:
- converting vague feature directions into structured research artifacts
- producing bounded candidate feature specs
- enforcing closed-bar timing and anti-leakage rules
- generating implementation-ready handoffs for another agent
- generating writer-facing per-feature guidelines as a separate artifact
- writing reproducible records into `research_library/ohlcv_feature_engineering/`

Assume the user will often start with a natural-language feature idea, not a formal spec.
Your first job is to convert that idea into:
- parsed direction metadata
- explicit hypotheses
- a small set of candidate features
- a hard timing audit
- a quality screen
- an implementation handoff
- a library record

## Working Directory

- Always work from the repo root:
  - `/Users/zhaoyub/Documents/Tradings/SoionLab`
- Run Python commands with:
  - `PYTHONPATH=src`
- Treat `skills/OHLCV_feature_engineering/` as the reusable skill package.
- Treat `research_library/ohlcv_feature_engineering/` as the durable storage location.
- Use a two-level storage rule:
  - canonical deduplicated artifacts under `specs/`, `reports/`, and `guidelines/`
  - per-run bundles under `runs/YYYYMMDD/<record_id>/`

## Read First

Before running the workflow, inspect these:
- `skills/OHLCV_feature_engineering/references/strategy_roadmap.md`
- `skills/OHLCV_feature_engineering/roadmap.md`
- `skills/OHLCV_feature_engineering/runner.md`
- `skills/OHLCV_feature_engineering/TIMING_RULES.md`
- `skills/OHLCV_feature_engineering/schemas/`
- `skills/OHLCV_feature_engineering/scripts/run_pipeline.py`
- the relevant runtime and feature contracts under `src/quant_engine/`

Use `roadmap.md` for repo placement and output conventions.
Use `runner.md` when you need a compact prompt/checklist for a concrete feature-research request.
Use `TIMING_RULES.md` when a direction risks leaking future information or confusing poll time with semantic time.

Reusable skill-side helpers live under:
- `skills/OHLCV_feature_engineering/scripts/`

Templates and schema references live under:
- `skills/OHLCV_feature_engineering/scripts/templates/`
- `skills/OHLCV_feature_engineering/schemas/`

## Skill Package Layout

Reusable pipeline entrypoints:
- `skills/OHLCV_feature_engineering/scripts/direction_parsing.py`
- `skills/OHLCV_feature_engineering/scripts/feature_synthesis.py`
- `skills/OHLCV_feature_engineering/scripts/timing_audit.py`
- `skills/OHLCV_feature_engineering/scripts/family_classification.py`
- `skills/OHLCV_feature_engineering/scripts/quality_evaluation.py`
- `skills/OHLCV_feature_engineering/scripts/handoff_builder.py`
- `skills/OHLCV_feature_engineering/scripts/library_writer.py`
- `skills/OHLCV_feature_engineering/scripts/run_pipeline.py`
- `skills/OHLCV_feature_engineering/scripts/target_construction.py`
- `skills/OHLCV_feature_engineering/scripts/feature_matrix_alignment.py`
- `skills/OHLCV_feature_engineering/scripts/ic_analysis.py`
- `skills/OHLCV_feature_engineering/scripts/stability_diagnostics.py`
- `skills/OHLCV_feature_engineering/scripts/split_generation.py`
- `skills/OHLCV_feature_engineering/scripts/walk_forward_validation.py`
- `skills/OHLCV_feature_engineering/scripts/redundancy_analysis.py`
- `skills/OHLCV_feature_engineering/scripts/report_synthesis.py`
- `skills/OHLCV_feature_engineering/scripts/run_evaluation_pipeline.py`

## Standard Workflow

Follow this order unless the user explicitly narrows scope.

### 1. Discovery

Identify:
- the exact OHLCV direction being requested
- the likely signal family
- the intended use context if known: `MODEL`, `DECISION`, or `RISK`
- the observation interval
- whether optional OHLCV aux fields are needed
- whether a reference symbol is required

Do not synthesize features before you know:
- the semantic interval
- the visible data boundary
- the core inputs
- the feature-count budget

### 2. Direction parsing

Run:

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/direction_parsing.py \
  --input <input.json> \
  --output <workdir>/parsed_directions.json
```

This stage converts natural language into:
- `parsed_directions`
- `hypotheses`

### 3. Feature synthesis

Run:

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/feature_synthesis.py \
  --input <workdir>/parsed_directions.json \
  --output <workdir>/candidate_features.json
```

Rules:
- keep the candidate set small
- prefer direct, interpretable formulas
- do not generate a feature bank

### 4. Timing audit

Run:

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/timing_audit.py \
  --input <workdir>/candidate_features.json \
  --output <workdir>/audited_features.json
```

This is a hard gate.
Reject or clarify any candidate that:
- uses future bars
- depends on partially formed bars
- confuses ingestion cadence with semantic observation time
- requires unavailable fields without explicitly marking them

### 5. Family classification

Run:

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/family_classification.py \
  --input <workdir>/audited_features.json \
  --output <workdir>/classified_features.json \
  --yaml-output <workdir>/feature_specs.yaml
```

Use only these canonical families:
- `price_path`
- `volatility`
- `volume_liquidity`
- `order_flow_proxy`
- `inventory_state`
- `trade_activity`
- `time_structure`
- `cross_interaction`

For `inventory_state`, use this exact anchor contract:
- `typical_price_t = (high_t + low_t + close_t) / 3`
- `refresh_t = min(refresh_cap, quote_asset_volume_t / (sum(quote_asset_volume_i, i=t-w_refresh+1..t) + eps))`
- `anchor_t = (1 - refresh_t) * anchor_{t-1} + refresh_t * typical_price_t`
- seed `anchor_{t0} = typical_price_{t0}` at the first bar `t0` where the trailing refresh window is valid
- bindable strategy params must be exposed explicitly, at minimum `refresh_window_bars`, `refresh_cap`, and any downstream normalization window such as `z_window_bars` or `range_window_bars`

### 6. Quality evaluation

Run:

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/quality_evaluation.py \
  --input <workdir>/classified_features.json \
  --json-output <workdir>/quality_report.json \
  --md-output <workdir>/quality_report.md
```

This stage handles:
- within-family redundancy control
- fixed-dimension scoring
- keep/defer/reject verdicts
- explicit feature-role assignment support for downstream evaluation

### 7. Handoff build

Run:

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/handoff_builder.py \
  --features <workdir>/classified_features.json \
  --quality <workdir>/quality_report.json \
  --output <workdir>/implementation_handoff.md
```

Do not implement feature code here.
The handoff must be exact enough for another agent to implement under `src/quant_engine/features/`.

### 8. Library writeback

Run:

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/library_writer.py \
  --parsed <workdir>/parsed_directions.json \
  --features-json <workdir>/classified_features.json \
  --features-yaml <workdir>/feature_specs.yaml \
  --quality-md <workdir>/quality_report.md \
  --handoff-md <workdir>/implementation_handoff.md \
  --output <workdir>/library_record.json
```

This stage must:
- preserve append-only registry behavior
- avoid silent overwrites
- perform basic deduplication
- always write a self-contained per-run bundle under `research_library/ohlcv_feature_engineering/runs/YYYYMMDD/<record_id>/`
- keep canonical deduplicated artifacts separate from run-local copies so repeated future runs remain easy to inspect

### 9. Preferred single-command entrypoint

For normal use, prefer:

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/run_pipeline.py \
  --input <input.json> \
  --output-dir <workdir>
```

Expected final files:
- `<workdir>/parsed_directions.json`
- `<workdir>/feature_specs.yaml`
- `<workdir>/quality_report.md`
- `<workdir>/implementation_handoff.md`
- `<workdir>/writer_guidelines.md`
- `<workdir>/library_record.json`

## Evaluation Workflow

Use this when a user or agent needs feature-research evidence beyond definitions.
This workflow evaluates features, not strategies.

### 1. Target construction

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/target_construction.py \
  --input <matrix.csv|parquet> \
  --output <evaldir>/target_matrix.csv \
  --horizon-bars 1 \
  --target-kind forward_return
```

### 2. Feature matrix alignment

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/feature_matrix_alignment.py \
  --input <evaldir>/target_matrix.csv \
  --output <evaldir>/aligned_feature_matrix.csv \
  --features <feature_a,feature_b,...> \
  --target-column target_forward_return_1
```

### 3. IC analysis

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/ic_analysis.py \
  --input <evaldir>/aligned_feature_matrix.csv \
  --output <evaldir>/ic_analysis.json \
  --features <feature_a,feature_b,...> \
  --target-column target_forward_return_1 \
  --ic-kind auto
```

Rules:
- default to time-series IC unless the matrix explicitly supports cross-sectional IC
- distinguish time-series and cross-sectional IC in the output
- do not assume cross-sectional support from this repo by default

### 4. Stability diagnostics

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/stability_diagnostics.py \
  --input <evaldir>/aligned_feature_matrix.csv \
  --output <evaldir>/stability_diagnostics.json \
  --features <feature_a,feature_b,...> \
  --target-column target_forward_return_1
```

### 5. IS/OOS split generation

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/split_generation.py \
  --input <evaldir>/aligned_feature_matrix.csv \
  --output <evaldir>/splits.json \
  --train-bars 32 \
  --test-bars 16 \
  --step-bars 16 \
  --mode rolling
```

### 6. Walk-forward validation

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/walk_forward_validation.py \
  --input <evaldir>/aligned_feature_matrix.csv \
  --splits <evaldir>/splits.json \
  --output <evaldir>/walk_forward_validation.json \
  --features <feature_a,feature_b,...> \
  --target-column target_forward_return_1 \
  --ic-kind auto
```

### 7. Redundancy analysis

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/redundancy_analysis.py \
  --input <evaldir>/aligned_feature_matrix.csv \
  --output <evaldir>/redundancy_analysis.json \
  --features <feature_a,feature_b,...> \
  --quality-input <evaldir>/../empirical_quality.json
```

Rules:
- hard duplicates produce a single representative only
- soft groups preserve one representative per mechanism class when mechanisms differ
- temporal features default to `research_only`
- temporal features may move to `implement_first` only if they exceed non-temporal candidates on both significance and baseline lift

### 8. Evaluation report synthesis

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/report_synthesis.py \
  --ic <evaldir>/ic_analysis.json \
  --stability <evaldir>/stability_diagnostics.json \
  --walk-forward <evaldir>/walk_forward_validation.json \
  --redundancy <evaldir>/redundancy_analysis.json \
  --output <evaldir>/evaluation_quality_report.md
```

### 9. Preferred single-command evaluation entrypoint

```bash
PYTHONPATH=src python skills/OHLCV_feature_engineering/scripts/run_evaluation_pipeline.py \
  --input <matrix.csv|parquet> \
  --output-dir <evaldir> \
  --features <feature_a,feature_b,...> \
  --horizon-bars 1
```

## Hard Constraints

1. Do NOT implement feature code in this skill.
2. Do NOT produce strategy entry or exit logic.
3. Do NOT use future data, future labels, or forward returns.
4. Do NOT treat ingestion poll cadence as semantic time.
5. Do NOT produce uncontrolled candidate explosions.
6. Do NOT skip the timing audit.
7. Do NOT skip library writeback.

## Output Contract

Final deliverables must include:
- `parsed_directions.json`
- `feature_specs.yaml`
- `quality_report.md`
- `implementation_handoff.md`
- `library_record.json`
- `output_sets` with:
  - `implement_first`
  - `research_only`
  - `archive_only`

Intermediate files may include:
- `candidate_features.json`
- `audited_features.json`
- `classified_features.json`
- `quality_report.json`
- `redundancy_analysis.json` with:
  - hard `clusters`
  - soft `mechanism_representatives`
  - `output_sets`

## Repo Notes

- OHLCV timing semantics are enforced by the runtime and handler contracts:
  - Driver owns `step_ts`
  - OHLCV visibility is `data_ts <= visible_end_ts(step_ts)`
  - features must be computable from closed bars only
- Strategy declarations belong in `apps/strategy/`
- Feature implementations belong in `src/quant_engine/features/`
- This skill produces research artifacts and implementation instructions only

## Templates And Schemas

Useful bundled files:
- `skills/OHLCV_feature_engineering/scripts/templates/example_direction.json`
- `skills/OHLCV_feature_engineering/scripts/templates/example_feature_matrix.csv`
- `skills/OHLCV_feature_engineering/scripts/templates/quality_report_template.md`
- `skills/OHLCV_feature_engineering/scripts/templates/implementation_handoff_template.md`
- `skills/OHLCV_feature_engineering/scripts/templates/evaluation_report_template.md`
- `skills/OHLCV_feature_engineering/schemas/direction_input.schema.json`
- `skills/OHLCV_feature_engineering/schemas/parsed_output.schema.json`
- `skills/OHLCV_feature_engineering/schemas/feature_specs.schema.json`
- `skills/OHLCV_feature_engineering/schemas/library_record.schema.json`

## Final Rule

This skill is a deterministic research pipeline package.

It must always prefer:
- scripts over prompt logic
- structure over freeform ideation
- causal safety over speculative predictive claims
- reusable outputs over one-off prose
