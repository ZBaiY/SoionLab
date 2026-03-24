# Metric Definitions

Use these defaults unless the user asks for something else.

## Primary Metrics

### Total return
- Slice-local equity change over the window.

### Sharpe
- Compute from step-to-step equity returns on the sliced equity curve.
- Use it as a quality measure, not as proof of robustness by itself.

### Max drawdown
- Compute from the sliced equity curve peak-to-trough path.

## Behavior Metrics

### Entry frequency
- Prefer buy-side entry count over raw sell fill count.

### Time in market
- Fraction of steps with positive position quantity.

### Holding time
- Use completed round trips when available, or approximate from position-state changes.

### Hit rate
- Use only if trade reconstruction is reasonably clean.
- If trade artifacts contain dust or residual bookkeeping noise, say so.

## Important Warnings

- Raw sell fill count is often polluted by clipping, flattening, or residual cleanup.
- Reconstructed trade count can still be noisy if tiny residual fills exist.
- If frequency metrics are noisy, prioritize:
  - buy-side entries
  - holding time
  - time in market
  - post-decision drift
