# OHLCV Handoff Implementation Roadmap

Internal handbook for converting an approved research run bundle into code, tests, and engineering-facing implementation records in the SoionLab repo.

## 1. Scope

This skill does not produce research.
It consumes a completed research handoff and produces:
- implementation plan
- implementation progress log
- code changes
- focused tests
- validation results
- implementation report
- changed-files record

Use it when the task is already in implementation mode.
If the handoff is not precise enough to implement without guessing, stop and ask.

## 2. Placement

Reusable skill guidance belongs under:

```text
skills/ohlcv-handoff-implementation/
  SKILL.md
  roadmap.md
  runner.md
  scripts/
    init_implementation_workspace.py
    validate_run_bundle.py
    validate_implementation_workspace.py
    templates/
  schemas/
    run_bundle_readset.schema.json
    implementation_workspace.schema.json
  agents/
```

Research-to-engineering handoff input lives under:

```text
research_library/ohlcv_feature_engineering/runs/YYYYMMDD/<record_id>/
  feature_specs.yaml
  implementation_handoff.md
  writer_guidelines.md
  quality_report.md
  redundancy_analysis.json
  library_record.json
```

Per-run engineering outputs belong next to the handoff, not in canonical research stores:

```text
research_library/ohlcv_feature_engineering/runs/YYYYMMDD/<record_id>/
  implementation/
    implementation_plan.md
    implementation_progress.md
    implementation_report.md
    test_report.md
    changed_files.md
```

Layout rules:
- treat the run bundle as the API window between research and implementation
- read from the run bundle first
- write implementation-facing notes only into `implementation/`
- do not overwrite canonical research artifacts during implementation
- if implementation reveals a research mismatch, write it into `implementation/implementation_report.md`

## 3. Required Read Order

Read these artifacts in this order:

1. source contract
   - `feature_specs.yaml`
2. implementation handoff
   - `implementation_handoff.md`
3. writer guideline
   - `writer_guidelines.md`
4. quality result
   - `quality_report.md`
   - or empirical evaluation output if present instead
5. selection / redundancy result
   - `redundancy_analysis.json`
   - and grouped output sets if present
6. library record
   - `library_record.json`

Minimal engineering read set:
- `feature_specs.yaml`
- `implementation_handoff.md`
- `writer_guidelines.md`

Only after that, inspect the repo contracts and tests that own the target behavior.

## 4. Repo Grounding

Inspect the narrowest code surface that governs the task.

For feature implementation, read first:
- `src/quant_engine/contracts/feature.py`
- `src/quant_engine/features/extractor.py`
- `src/quant_engine/features/registry.py`
- the nearest existing implementations under `src/quant_engine/features/`
- the nearest focused tests under `tests/unit/`, `tests/contract/`, or `tests/runtime/`

For strategy/runtime implementation, read the nearest owning files under:
- `apps/strategy/`
- `src/quant_engine/strategy/`
- `src/quant_engine/runtime/`
- `src/quant_engine/data/`
- the nearest focused tests

Do not read broadly.
Read the owner files, protocol boundaries, and the closest tests.

## 5. Standard Workflow

### 5.1 Confirm the implementation window

Identify:
- the exact run bundle
- the exact owner files
- the exact test owner path
- whether the work must stay private and git-untracked
- whether the task is additive, corrective, or both

### 5.2 Create the implementation workspace

Create:
- `implementation_plan.md`
- `implementation_progress.md`
- `implementation_report.md`
- `test_report.md`
- `changed_files.md`

Use this workspace as the engineering audit surface for that one run only.

Preferred initialization command:

```bash
python3 skills/ohlcv-handoff-implementation/scripts/init_implementation_workspace.py \
  --run-bundle <RUN_BUNDLE_PATH>
```

### 5.3 Plan before code

Document:
- scope
- target files
- required invariants
- hot-path complexity target
- memory boundaries
- private/public placement decision
- exact tests to write first
- explicit deferrals

### 5.4 Use TDD

Write tests before implementation.

Order:
1. registration or wiring tests
2. correctness tests
3. incremental semantics tests
4. determinism or replay tests
5. then implementation

Test rules:
- keep tests focused and deterministic
- match the nearest repo test style
- assert warmup behavior when stateful logic exists
- assert pre-history `None` behavior when the contract requires it
- assert timing and closed-bar semantics when the task is time-sensitive
- assert replay determinism for stateful logic

### 5.5 Implement minimally

Default rule:
- modify existing logic in place

Only add files when:
- the current owner surface cannot hold the change cleanly
- a private asset boundary requires an ignored tree
- or the handoff explicitly requires a new organized family area

### 5.6 Validate in layers

Run focused tests first, then widen only as needed:
- target test module
- nearest unit or contract tests
- nearest runtime tests if lifecycle or timing is involved

Validate the input bundle before work:

```bash
python3 skills/ohlcv-handoff-implementation/scripts/validate_run_bundle.py \
  --run-bundle <RUN_BUNDLE_PATH>
```

Validate the engineering output after work:

```bash
python3 skills/ohlcv-handoff-implementation/scripts/validate_implementation_workspace.py \
  --run-bundle <RUN_BUNDLE_PATH>
```

### 5.7 Write engineering outputs

Keep:
- `implementation_progress.md` updated as work advances
- `test_report.md` with exact commands and results
- `changed_files.md` with final file list and purpose
- `implementation_report.md` with the final step-by-step record and residual risks

## 6. Implementation Boundaries

### No abstraction inflation

By default:
- no new classes
- no new modules
- no new background workers
- no new global state
- no new long-lived attributes
- no new queues
- no new caches
- no new helper utilities unless strictly unavoidable

Prefer modifying existing logic in place.

If you introduce a new abstraction, explicitly prove:
- which invariant blocks minimal repair
- why the current structure cannot enforce it
- why the abstraction is strictly required

### No symptom masking

Do not:
- add silent fallbacks
- add catch-and-continue
- add retry to hide deterministic bugs
- add jitter to hide structural contention
- add exit logic to hide deadlock
- add buffering to hide ordering bugs
- add copying to hide ownership violations unless proven necessary

If the symptom disappears but the invariant remains broken, the repair is invalid.

### Preserve semantics

Do not change:
- business logic
- data shape
- timing contracts
- ordering guarantees
- partitioning logic

Unless an invariant requires it.

## 7. Exception Handling Gate

Before adding any `try/except`, complete all gates:

1. locate the raiser
   - exact raising statement
   - exact exception type
   - exact violated precondition
   - full minimal call chain
2. classify the cause
   - A) internal invariant violation
   - B) external transient failure
   - C) external persistent unrecoverable failure
3. if catch is used, prove it is necessary
   - which invariant cannot be enforced internally
   - why enforcing earlier violates hard constraints
   - why failing fast is correct
   - what external actor must change
   - why this does not mask the root cause
4. guarantee exit safety
   - no blocking left behind
   - no child waiting indefinitely
   - no resource leak
   - no `join()` without timeout
   - bounded-time `SIGINT` termination

Only `B` and `C` allow controlled catch.
`A` forbids catch as the primary fix.

## 8. Invariant, Complexity, and Memory Discipline

For every issue or contract mismatch, state:
- what invariant is violated
- where that invariant should have been enforced
- why the current layer failed to enforce it
- why it surfaces at the boundary
- whether it is memory-bound, ownership-bound, lock-bound, retry-amplified, IO-bound, scheduling-bound, ordering-bound, or resource-bound

State complexity before and after:
- no `O(n²)` in the hot path
- prefer `O(1)`, `O(log n)`, or `O(n)`
- no hidden scans
- no hidden blocking

State memory behavior:
- no unbounded containers
- no hidden retention
- no silent buffer growth
- no excessive copying
- explain object lifetime
- distinguish peak from steady-state memory

Reason about IO explicitly:
- burst amplification
- lock and IO coupling
- backpressure behavior
- retry amplification
- external contract violation

## 9. Private And Proprietary Implementations

If the implementation must stay untracked:
- keep proprietary code in an ignored tree
- keep proprietary tests in an ignored tree
- keep public tracked changes minimal and structural only

For private feature work in this repo, the proven pattern is:
- source under `apps/features/<domain>/`
- thin bootstrap under `apps/strategy/`
- tests under an ignored `tests/private_<slug>/`

Before using that pattern:
- confirm `.gitignore` already protects the tree, or add the narrowest ignore rule
- avoid widening the public registry or loader unless the task requires it
- keep the bootstrap thin and local

## 10. Repo-Specific Pitfalls

Do not repeat these mistakes:

- in `src/quant_engine/features/extractor.py`, `_` is the structural separator in feature names
  - `<TYPE>_<PURPOSE>_<SYMBOL>`
  - `<TYPE>_<PURPOSE>_<SYMBOL>^<REF>`
  - therefore multi-word `TYPE` tokens must not use `_`
- `initialize()` and `update()` are different contracts
  - `initialize()` handles cold start and warmup
  - `update()` must stay incremental where possible
- when testing handler-driven logic
  - align handlers before ingesting synthetic ticks
  - put synthetic timestamps on valid interval boundaries
  - do not confuse poll cadence with semantic observation time
- for OHLCV snapshots
  - auxiliary fields may live under snapshot `aux`, not direct attributes
- for extractor cold start
  - verify current initialization behavior against the contract and nearest tests before assuming exact timestamp semantics

## 11. Validation Standard

Default command:

```bash
PYTHONPATH=src pytest -q <targeted_test_path>
```

Then expand only as needed:

```bash
PYTHONPATH=src pytest -q tests/unit/...
PYTHONPATH=src pytest -q tests/contract/...
PYTHONPATH=src pytest -q tests/runtime/...
```

An implementation run is valid only if:
- code changes follow the approved handoff
- tests were written before implementation
- focused validation is green
- implementation notes exist under the run-local `implementation/` directory
- test commands and outcomes were recorded
- the run bundle and implementation workspace both validate cleanly
