# OHLCV Timing Rules

Use this reference when a feature direction risks violating repo timing semantics.

## Runtime contract

- Driver owns `step_ts`
- observation interval is semantic time
- OHLCV bars are visible only after close

For OHLCV:

```text
visible_end_ts(step_ts) = floor(step_ts / interval_ms) * interval_ms - 1
```

Valid feature inputs must satisfy:

```text
data_ts <= visible_end_ts(step_ts)
```

## Allowed

- trailing rolling windows
- past-only normalization
- closed-bar OHLCV fields
- optional OHLCV aux fields when explicitly declared
- cross-symbol trailing spreads when both symbols use aligned closed bars

## Not allowed

- next-bar return
- future volatility
- future extrema
- partial-bar values
- poll-cadence bucketing
- dataset-wide normalization fit on future data

## Research implication

If a feature idea only becomes meaningful after observing the next bar, it is not a valid decision-time feature in this repo.

Evaluation targets may use future returns, but only as research labels outside the feature-definition path.
Do not mix future-looking targets into feature formulas or implementation handoffs.
