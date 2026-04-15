---
name: strategy-writer
description: Use this skill when implementing, organizing, testing, evaluating, or reporting on trading strategies in this repository. Covers family-based placement under apps/strategy, focused strategy edits, reusable research runner setup, parameter scans, IS/OOS comparison, and strategy-facing documentation. Do not use it for unrelated engine refactors or feature-research generation.
---

# Strategy Writer

Use this skill for strategy work in this repo:
- add or refine a strategy family
- reorganize strategy declarations into repo-consistent family layout
- add focused strategy tests
- create or update reusable research runners
- run parameter scans and IS/OOS comparisons
- write family-local research notes and reports

This skill does not invent unrelated architecture.
It keeps strategy work local to the owning family and the existing engine contracts.

## Working Directory

- Always work from the repo root:
  - `/Users/zhaoyub/Documents/Tradings/SoionLab`
- Run Python and pytest with:
  - `PYTHONPATH=src`
- Treat `apps/strategy/` as the strategy declaration surface.
- Treat `tests/unit/strategies/` as the focused strategy validation surface.
- Treat `docs/strategies/<family_slug>/` as the family-local research surface.

## Read First

Before editing code, inspect these:
- `skills/Strategy_Writer/references/strategy_roadmap.md`
- `skills/Strategy_Writer/roadmap.md`
- `skills/Strategy_Writer/runner.md`
- `skills/Strategy_Writer/OHLCV_GAP_HANDLING.md` when backtest OHLCV gap handling matters
- the owning strategy family under `apps/strategy/families/`
- the nearest focused strategy tests
- the matching decision/model/risk/execution owners under `src/quant_engine/`

Use `roadmap.md` for placement, family structure, validation order, and research conventions.
Use `runner.md` for a compact implementation/research prompt.
Use `scripts/` to scaffold family layout or run generic research workflows.
Use `schemas/` when you need the family layout or research config in machine-readable form.

## Skill Package Layout

Reusable strategy-workflow guidance belongs under:

```text
skills/Strategy_Writer/
  SKILL.md
  roadmap.md
  runner.md
  scripts/
  schemas/
  agents/
```

## Standard Workflow

Follow this order unless the user narrows scope:

1. Identify the owning family and nearest reference strategies.
2. Inspect the narrowest code and test surface.
3. Keep strategy declarations organized by family under `apps/strategy/families/`.
4. Write or update focused tests first.
5. Implement the smallest strategy-local change.
6. Use reusable skill scripts for research scaffolding or generic scan workflows.
7. Keep family-specific research artifacts under `docs/strategies/<family_slug>/`, not in the skill package.

## Reusable Scripts

Use these when helpful:
- `scripts/init_strategy_family.py`
  - creates family package, docs folder, and focused test placeholder
- `scripts/strategy_research_layout.py`
  - prints the expected family layout in this repo
- `scripts/run_strategy_research.py`
  - runs a generic config-driven strategy scan and comparison workflow
- `scripts/common_research.py`
  - reusable helpers for backtest runs, aggregation, comparisons, and output writing

## Machine-Readable Contracts

Schemas live under `schemas/`:
- `strategy_family_layout.schema.json`
- `strategy_research_config.schema.json`

## Acceptance Standard

A strategy run is valid only if:
- strategy code is organized under the correct family
- public compatibility imports still work where required
- focused tests cover the changed behavior
- family-specific research logic lives with the family, not inside the generic skill
- validation commands and research outputs are recorded in the family-local docs area
