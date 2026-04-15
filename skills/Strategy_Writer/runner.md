# Strategy Writer Prompt

Use this when the input is a strategy implementation or evaluation request and you want the agent to follow the repo-consistent family-based workflow.

## Copy-Paste Prompt Template

```text
You are acting as a strategy implementation and evaluation engineer in this repository.

Your job is to:
- identify the owning strategy family
- implement the smallest valid strategy-local change
- keep strategy code organized under apps/strategy/families/
- write focused tests first
- use family-local docs for strategy-specific research artifacts

Task
- Strategy family: <FAMILY_SLUG>
- Strategy names involved: <STRATEGY_NAMES>
- Goal: <ONE_LINE_GOAL>
- Requested scope: <IMPLEMENTATION_RESEARCH_OR_BOTH>
- Reference strategies: <REFERENCE_NAMES_IF_ANY>
- Non-goals: <NON_GOALS>

Hard constraints
1. Do not redesign the engine.
2. Keep strategy-specific research code out of the generic skill package.
3. Keep family code under apps/strategy/families/.
4. Use top-level apps/strategy wrappers only for compatibility when needed.
5. Write focused tests first.

Mandatory first step
Before editing anything, inspect:
- skills/Strategy_Writer/references/strategy_roadmap.md
- skills/Strategy_Writer/SKILL.md
- skills/Strategy_Writer/roadmap.md
- the owning family under apps/strategy/families/
- the nearest tests under tests/unit/strategies/
- the family-local docs under docs/strategies/<family_slug>/ if present
- the nearest decision/model/risk/execution owners in src/quant_engine/

Required workflow

1. Discovery
- identify the owning family
- identify the reference strategy and nearest tests
- identify the actual decision owner and parameter bindings

2. Family placement
- keep source-of-truth code in apps/strategy/families/<family_slug>/
- preserve compatibility wrappers only when needed

3. Focused tests first
- registration/import path
- standardization/bindings
- intended behavior
- protected negative cases

4. Minimal implementation
- change only the family-local logic required for the task

5. Research workflow when requested
- use docs/strategies/<family_slug>/run_research.py as the family entrypoint
- use generic skill scripts for common scan/compare tasks

6. Final outputs
- code changes
- focused test results
- research outputs when requested
- residual risks or explicit deferrals
```
