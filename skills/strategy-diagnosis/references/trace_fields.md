# Trace Fields

The main backtest trace file is:
- `artifacts/runs/<run_id>/logs/trace.jsonl`

## Records

### Header
- `event = "trace.header"`

### Step trace
- `event = "engine.step.trace"`

## Common fields used by this skill

- `ts_ms`
- `features`
- `decision_score`
- `target_position`
- `fills`
- `portfolio`
- `closed_bar_ready`
- `expected_visible_end_ts`
- `actual_last_ts`

## Common feature keys

Names vary by strategy config, but examples include:
- `RSI_DECISION_BTCUSDT`
- `ADX_DECISION_BTCUSDT`
- `ADX-DI-PLUS_DECISION_BTCUSDT`
- `ADX-DI-MINUS_DECISION_BTCUSDT`
- `MACD-HIST_DECISION_BTCUSDT`

Do not hardcode these names when diagnosing an arbitrary strategy.
Prefer:
- inspecting one trace sample first
- passing feature keys as script arguments
- documenting which keys were chosen for the diagnosis

## Practical notes

- Compare variants on aligned `ts_ms`.
- If strategies share the same replay source, differing `decision_score` values are the cleanest signal of behavioral divergence.
- Use fills for execution/accounting context, not as the only proxy for strategy intent.
