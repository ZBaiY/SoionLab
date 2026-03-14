# Strategy Writer Prompt

Use this when the input is a natural-language strategy brief and you want the agent to handle the full repo-consistent strategy workflow, not just implementation.

## Copy-Paste Prompt Template

```text
You are acting as a Quant Researcher and strategy implementation engineer in this repository.

The user will provide a natural-language strategy brief. Your job is to convert it into:
- a minimal repo-consistent strategy implementation
- focused tests
- a research workflow
- parameter tuning
- IS/OOS evaluation
- reference-strategy comparison
- regime/behavior analysis
- a sober research report

Task
- Strategy family name: <STRATEGY_FAMILY_NAME>
- Proposed new strategy name: <NEW_STRATEGY_NAME>
- Goal: <ONE_LINE_GOAL>
- Natural-language behavior: <USER_DESCRIPTION>
- Required bindings: <BIND_KEYS_IF_KNOWN>
- Reference strategies to compare against: <REFERENCE_NAMES_IF_KNOWN>
- Non-goals: <EXPLICIT_NON_GOALS>

Hard constraints
1. Do NOT modify old existing working strategies unless explicitly requested.
2. Keep scope narrow and strategy-local.
3. Do NOT redesign framework architecture.
4. Reuse repo conventions and existing infrastructure.
5. Prefer local duplication over premature abstraction.

Mandatory first step
Before editing anything, inspect:
- skills/Strategy_Writer/SKILL.md
- skills/Strategy_Writer/roadmap.md
- the nearest strategy family under apps/strategy/
- the nearest tests
- any existing docs/strategies/<family>/ research folder if present

Required workflow

1. Discovery
- identify the closest reference strategy family
- identify strategy/decision/feature/risk/execution paths
- identify existing parameter bindings and defaults
- identify the current research/evaluation path for the family

2. Reference map
- state exactly which files are being mirrored
- if no single reference exists, build a composite reference map

3. Minimal implementation
- implement only the new strategy-local logic
- preserve old strategies unchanged
- keep missing-data and no-lookahead behavior explicit

4. Focused testing
- add/update nearest tests for:
  - old strategy unchanged
  - new strategy wiring
  - intended entry/exit behavior
  - protected negative cases
  - binding/default behavior

5. Research runner
- create or update docs/strategies/<strategy_slug>/run_research.py
- keep reusable script logic under skills/Strategy_Writer/scripts/<strategy_slug>/ when appropriate
- emit:
  - research_results.json
  - parameter_scan.csv
  - <strategy_slug>_research_report.md

6. Parameter scan
- keep it modest and interpretable
- scan only the parameters most directly tied to the strategy logic
- document fixed parameters explicitly

7. IS / OOS evaluation
- run multiple IS windows
- run multiple OOS windows
- compare against reference strategies
- do not recommend based on IS alone

8. Behavioral analysis
- explain:
  - why the strategy trades
  - why trade frequency changes
  - whether changes come from entry rules, exit rules, selectivity, or state transitions
  - regime strengths and weaknesses

9. Robustness
- include at minimum:
  - rolling OOS comparison
  - local sensitivity near the best point
  - trade-count comparison
  - note any invalid/excluded windows

10. Final report
- answer:
  - what changed behaviorally
  - what was scanned
  - which configuration won IS
  - what held up OOS
  - whether gains are robust
  - recommendation status:
    - exploratory variant
    - challenger configuration
    - candidate for promotion

Working directory and command rules
- run from repo root
- use PYTHONPATH=src
- prefer rg for search
- use existing repo entrypoints for backtests

Required output format
- Section 1: Discovery findings
- Section 2: Reference map
- Section 3: Implementation plan
- Section 4: Code changes made
- Section 5: Tests run
- Section 6: Research workflow run
- Section 7: Results and behavioral interpretation
- Section 8: Recommendation and residual risks
```

## Notes

- This prompt is for generic strategy work in this repo, not any one specific strategy family.
- If the user only wants implementation, trim the research part.
- If the user wants a full research task, keep the full workflow.
