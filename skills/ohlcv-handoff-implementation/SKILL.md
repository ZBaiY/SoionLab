---
name: ohlcv-handoff-implementation
description: Use this skill when a user or agent needs to turn an approved OHLCV research run bundle into code, tests, and implementation-facing notes in this repository. Covers exact artifact read order, per-run implementation workspace, repo contract inspection, TDD-first execution, focused validation, and private/untracked implementation placement when assets must remain proprietary. Do not use it to produce research artifacts or invent new designs.
---

# OHLCV Handoff Implementation

Use this skill for implementation work that starts from an existing research bundle:
- the specs and formulas are already approved
- the job is to implement code, tests, and engineering notes
- the implementation must remain consistent with repo contracts and runtime semantics

This skill does not generate research.
It translates an approved handoff into:
- code changes
- focused tests
- run-local implementation artifacts
- validation results

## Working Directory

- Always work from the repo root:
  - `/Users/zhaoyub/Documents/Tradings/SoionLab`
- Run Python and pytest with:
  - `PYTHONPATH=src`
- Treat `research_library/` as the handoff surface.
- Treat `src/quant_engine/` and `apps/` as the implementation surface.
- Treat `tests/` as the validation surface.

## Read First

Before editing code, inspect these:
- `skills/ohlcv-handoff-implementation/references/strategy_roadmap.md`
- `skills/ohlcv-handoff-implementation/roadmap.md`
- `skills/ohlcv-handoff-implementation/runner.md`
- the exact run bundle under `research_library/.../runs/YYYYMMDD/<record_id>/`
- the narrow repo contracts that own the target behavior

Use `roadmap.md` for placement, boundaries, and workflow order.
Use `runner.md` when you need a compact prompt/checklist for a concrete implementation request.
Use `scripts/` when you want to initialize or validate the workflow mechanically instead of by hand.
Use `schemas/` when you need the run-bundle or implementation workspace contracts in machine-readable form.

## Skill Package Layout

Reusable implementation workflow guidance lives under:

```text
skills/ohlcv-handoff-implementation/
  SKILL.md
  roadmap.md
  runner.md
  scripts/
  schemas/
  agents/
```

Per-run engineering outputs belong under the run bundle:

```text
research_library/ohlcv_feature_engineering/runs/YYYYMMDD/<record_id>/
  implementation/
    implementation_plan.md
    implementation_progress.md
    implementation_report.md
    test_report.md
    changed_files.md
```

## Standard Workflow

Follow this order unless the user explicitly narrows scope:

1. Read the run bundle in the required order.
2. Inspect the narrowest repo contracts and nearest tests.
3. Create the run-local `implementation/` workspace.
4. Write the implementation plan before code.
5. Write tests first.
6. Implement the smallest valid change.
7. Run focused validation.
8. Write run-local implementation and test reports.

## Reusable Scripts

Use these when helpful:
- `scripts/init_implementation_workspace.py`
  - creates the run-local `implementation/` directory
  - writes the standard markdown artifacts from stable templates
- `scripts/validate_run_bundle.py`
  - validates the input read-set and reports missing or malformed artifacts
- `scripts/validate_implementation_workspace.py`
  - validates the engineering output directory after implementation work

## Machine-Readable Contracts

Schemas live under `schemas/`:
- `run_bundle_readset.schema.json`
  - required and optional handoff artifacts
- `implementation_workspace.schema.json`
  - required engineering outputs for one implementation run

## Acceptance Standard

An implementation run is valid only if:
- the read order was respected
- code changes match the approved handoff
- tests were written before implementation
- hot-path and timing invariants were preserved
- implementation notes were written under the run-local `implementation/` directory
- validation commands and results were recorded
