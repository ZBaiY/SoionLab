---
name: strategy-diagnosis
description: Use this skill when diagnosing what a trading strategy is good at, where it loses money, which regime it fits, why sibling variants differ, or what targeted improvement to test next. Use it for general strategy review requests that need regime slicing, post-decision drift, trace comparison, behavior analysis, or head-to-head analysis of completed backtest artifacts under artifacts/runs/.
---

# Strategy Diagnosis

Use this skill to diagnose strategy behavior from completed backtest artifacts in this repo.

Preferred inputs:
- one or more run IDs under `artifacts/runs/`
- the strategy names or sibling variants to compare
- optional calendar windows to slice

Preferred artifact sources:
- `artifacts/runs/<run_id>/report/report.json`
- `artifacts/runs/<run_id>/report/summary.json`
- `artifacts/runs/<run_id>/logs/trace.jsonl`
- `artifacts/runs/<run_id>/logs/default.jsonl`

## Use This Workflow

Follow this order. Do not jump to strategy changes before the diagnosis steps are done.

### 1. Separate the question

Answer these independently:
- What market condition is the strategy implicitly designed for?
- Where does it actually make money?
- Where does it lose money?
- Are losses caused by entry logic, exit logic, regime gating, or sizing/execution?

If a user asks a vague question like “is this strategy good,” translate it into those four questions first.

### 2. Prefer full-run slicing over many sub-window reruns

Use completed full runs when possible. Slice `report.json` and `trace.jsonl` into windows/regimes instead of launching many narrow backtests.

Reason:
- it matches the production backtest path more closely
- it avoids fragile replay-boundary issues
- it keeps comparisons aligned on the same realized path

Use `scripts/slice_backtest_windows.py` for calendar slicing.

### 3. Label regimes before interpreting metrics

At minimum classify:
- trend up
- trend down
- sideways low-vol
- choppy high-vol
- breakout / post-breakout
- liquidation / panic reversal

Use `scripts/classify_regimes.py`.
Read `references/regime_definitions.md` before changing thresholds or definitions.
If the strategy does not expose ADX/DMI-style features, adapt the regime classifier inputs instead of forcing the current defaults.

### 4. Compute regime metrics

For each regime or slice, compare:
- total return
- Sharpe
- max drawdown
- hit rate if trade reconstruction is trustworthy enough
- holding time
- time in market
- entry frequency

Do not treat raw sell fill count as strategy frequency.
Prefer:
- buy-side entries
- holding time
- time in market
- round trips with a sensible notional threshold

Use:
- `scripts/summarize_trade_behavior.py`
- `scripts/slice_backtest_windows.py`
- `scripts/build_regime_matrix.py`
- `scripts/analyze_trade_clustering.py` when cooldown-style churn is a live hypothesis

Read `references/metric_definitions.md` if the user asks why a metric was chosen.

### 5. Diagnose behavior, not just aggregate metrics

For each weak regime, inspect:
- early entry
- early exit
- overstaying
- missed re-entry
- overtrading in chop
- undertrading in trend

Compare actual decision changes from trace logs, not just final backtest summaries.
Use `scripts/compare_variants.py` to locate timestamps where sibling variants diverge.

### 6. Run post-decision drift

For important events, measure forward return after:
- buy signal
- sell signal
- blocked buy
- blocked sell

Default horizons:
- `1, 3, 6, 12, 24` bars

Use `scripts/compute_post_decision_drift.py`.
This is the main guardrail against telling superficial stories from aggregate PnL alone.

### 6.5. Check trade clustering before cooldown ideas

If the likely issue is repeated re-entry into unresolved noise, run a clustering pass before proposing a cooldown.

At minimum compute for completed trades in the target regime:
- gap since previous exit in bars
- re-entry rank within a configurable cluster window
- PnL by re-entry rank
- expectancy by gap bucket

Use `scripts/analyze_trade_clustering.py`.
Default:
- `cluster_window_bars=20`
- gap buckets `0-5`, `6-10`, `11-20`, `21+`

If losses are concentrated in second/third-or-later re-entries or in short-gap buckets, say that explicitly. That is the evidence needed before recommending a cooldown as the first experiment.

### 7. Compare counterfactual variants

If the user has sibling strategies, compare them directly:
- same entry, different exit
- same exit, different entry
- with and without regime gate
- stronger vs weaker filter
- hold longer vs exit faster
- simpler vs more conditional logic

Use `scripts/compare_variants.py`.

If two variants differ at only a handful of timestamps, say so explicitly. Do not overstate the significance of performance deltas when decision deltas are sparse.

### 8. Build the regime matrix

Final diagnosis should include a table with:
- regime
- return
- Sharpe
- drawdown
- entries
- holding time / time in market
- typical failure mode
- likely improvement

Use `scripts/build_regime_matrix.py` to join regime labels with realized step returns.
Use `scripts/build_diagnosis_report.py` to assemble a markdown summary after the intermediate CSV/JSON artifacts exist.

### 9. Only then recommend changes

Map observed failure modes to targeted next experiments:
- premature exits in trend -> exit-release logic
- missed trend participation -> trend-entry module
- chop bleed -> stronger entry filter or gate
- late failure exits -> faster regime-failure condition

Prefer one or two concrete next experiments, not a large idea list.

## Standard Output

When possible, produce:
- `diagnosis_summary.json`
- `regime_slices.csv`
- `regime_labels.csv`
- `regime_matrix.csv`
- `decision_drift.csv`
- `variant_deltas.csv`
- `trade_behavior.csv`
- `trade_clustering.csv`
- `trade_clustering_rank_summary.csv`
- `trade_clustering_gap_summary.csv`
- `trade_clustering_summary.json`
- `trade_clustering_snippet.md`
- `diagnosis_report.md`

Place outputs under a strategy-local docs folder or a temporary analysis folder under `artifacts/`.

## Repo Notes

- Work from repo root: `/Users/zhaoyub/Documents/Tradings/SoionLab`
- Use `PYTHONPATH=src` for Python commands
- Backtest reports are generated from trace data by `src/analyze/backtest/reporter.py`
- Trace records are the authoritative source for step-by-step decision differences

## Scripts In This Skill

- `scripts/classify_regimes.py`
  - label each bar/window from a run into coarse regimes
  - feature names are configurable; do not assume RSI/ADX keys unless they actually exist
- `scripts/slice_backtest_windows.py`
  - compute calendar-window metrics from a full run
- `scripts/compute_post_decision_drift.py`
  - compute forward returns after buy/sell/blocked decisions
- `scripts/compare_variants.py`
  - compare two runs timestamp-by-timestamp on decisions and selected feature keys
- `scripts/summarize_trade_behavior.py`
  - derive entry frequency, time in market, holding stats, and buy-side activity
- `scripts/build_regime_matrix.py`
  - join regime labels with realized step returns into a regime matrix
- `scripts/analyze_trade_clustering.py`
  - annotate completed trades with gap/re-entry clustering metadata and summarize cooldown evidence
- `scripts/build_diagnosis_report.py`
  - combine prior outputs into a compact markdown report

## References

Read only what you need:
- `references/regime_definitions.md`
  - use when changing or explaining regime definitions
  - start with the default taxonomy, then adapt the indicators to the strategy family
- `references/metric_definitions.md`
  - use when comparing or defending metric choices
- `references/trace_fields.md`
  - use when inspecting trace payload structure
