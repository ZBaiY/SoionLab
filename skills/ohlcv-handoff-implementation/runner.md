# OHLCV Handoff Implementation Prompt

Use this when the input is an approved research run bundle and you want the agent to run the full repo-consistent implementation workflow.

## Copy-Paste Prompt Template

```text
You are acting as an implementation engineer in this repository.

The research is already done.
Your job is to turn one approved run bundle into:
- a run-local implementation plan
- tests written first
- the minimal valid code changes
- focused validation
- implementation-facing engineering notes

Task
- Run bundle root: <RUN_BUNDLE_PATH>
- Primary target area: <TARGET_CODE_PATH>
- Primary test area: <TARGET_TEST_PATH>
- Private asset requirement: <YES_OR_NO>
- Requested scope: <ADD_FIX_OR_BOTH>
- Explicit non-goals: <NON_GOALS>

Hard constraints
1. Do NOT redo the research workflow.
2. Do NOT guess when the handoff is underspecified.
3. Do NOT widen repo scope without proof.
4. Do tests first, then implementation.
5. Keep hot-path logic incremental when the contract allows it.
6. Write engineering artifacts into the run-local implementation directory.

Mandatory first step
Before doing the work, inspect:
- skills/ohlcv-handoff-implementation/references/strategy_roadmap.md
- skills/ohlcv-handoff-implementation/SKILL.md
- skills/ohlcv-handoff-implementation/roadmap.md
- the run bundle artifacts in this order:
  - feature_specs.yaml
  - implementation_handoff.md
  - writer_guidelines.md
  - quality_report.md
  - redundancy_analysis.json if present
  - library_record.json if present
- the narrow repo contracts and nearest tests that own the target behavior

Recommended setup commands
- validate the run bundle first:
  - `python3 skills/ohlcv-handoff-implementation/scripts/validate_run_bundle.py --run-bundle <RUN_BUNDLE_PATH>`
- create the implementation workspace:
  - `python3 skills/ohlcv-handoff-implementation/scripts/init_implementation_workspace.py --run-bundle <RUN_BUNDLE_PATH>`

Required workflow

1. Create the run-local implementation workspace
- implementation_plan.md
- implementation_progress.md
- implementation_report.md
- test_report.md
- changed_files.md

2. Write the implementation plan
- scope
- target files
- invariants
- complexity target
- memory boundaries
- private/public placement
- exact tests to write first

3. Write tests first
- registration or wiring
- correctness
- incremental semantics
- determinism or replay

4. Implement minimally
- modify existing logic in place unless a stronger boundary forces a new file

5. Validate in layers
- targeted tests first
- then nearest unit, contract, or runtime tests as required

6. Write final engineering outputs
- what changed
- why it changed
- what was tested
- residual risks or explicit deferrals

7. Validate the implementation workspace
- `python3 skills/ohlcv-handoff-implementation/scripts/validate_implementation_workspace.py --run-bundle <RUN_BUNDLE_PATH>`

Required output format
- Section 1: Run bundle summary
- Section 2: Implementation plan
- Section 3: Tests written first
- Section 4: Code changes
- Section 5: Validation results
- Section 6: Implementation artifact paths
```
