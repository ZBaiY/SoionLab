# OHLCV Gap Handling

## Purpose
Define the default `Strategy_Writer` reasoning model for **backtest-only** OHLCV history gaps in this repo.

This document exists because SoionLab already treats OHLCV continuity as safety-critical, but the current backtest behavior is binary:
- warmup rejects internal holes in required OHLCV history
- step-time gating aborts when the next required closed bar never arrives

That is safe, but too brittle for historical Binance-style maintenance gaps on 15m data.

## Scope
- Applies to **BACKTEST** and **SAMPLE** reasoning only.
- Applies to canonical cleaned OHLCV datasets loaded through the local file path.
- Does **not** redesign live mode, realtime mode, or paper trading.
- Does **not** permit padding or interpolation of canonical clean OHLCV.

## Repo Grounding
Current control points:
- `src/ingestion/ohlcv/source.py`
  - `OHLCVFileSource` loads local parquet, canonicalizes `data_ts`, sorts rows, and yields records.
  - It does **not** currently classify or segment gaps.
- `src/quant_engine/data/ohlcv/realtime.py`
  - `OHLCVDataHandler.on_new_tick()` annotates market metadata with `gap_type` via `classify_gap(...)`.
  - In backtest mode, `_maybe_backfill()` is skipped, so this is metadata only, not recovery.
- `src/quant_engine/strategy/engine.py`
  - `warmup_features()` treats OHLCV as a hard domain in backtest.
  - `_has_required_history()` already rejects internal holes when adjacent bars in the required window differ by more than `2 * interval_ms`.
- `src/quant_engine/runtime/backtest.py`
  - `_gate_required_ohlcv()` blocks each step until the closed bar needed for that step is visible.
  - Once ingestion is exhausted, `_raise_if_ohlcv_data_missing()` raises `RuntimeError("backtest.missing_data ...")`.
  - `_log_closed_bar_not_ready()` already marks `lag_ms >= 3 * interval_ms` as actionable in logs, but still does not create segment semantics.
- `src/analyze/backtest/reporter.py`
  - report generation can warn about missing bars in the traced step timeline, but this happens after the run and does not change runtime semantics.

Current repo-grounded audit summary:
- Backtest already has hard OHLCV continuity gates in `src/quant_engine/strategy/engine.py` and `src/quant_engine/runtime/backtest.py`, but no segment/episode semantics after a real gap.
- Local OHLCV parquet loading in `src/ingestion/ohlcv/source.py` canonicalizes and sorts `data_ts`, but does not classify or split gaps.
- Snapshot-level `gap_type` support already exists via `src/quant_engine/data/contracts/snapshot.py`, so the repo partially supports interval-aware gap reasoning already.
- The likely 15m failure mode today is either warmup rejection for internal holes or `backtest.missing_data` during step gating; there is no “end segment, rewarm, continue” path.
- The local cleaned datasets do contain multi-hour 15m gaps, but mostly in `2020-2021`, plus a smaller `2023` gap; not mainly `2022/2024`.

## Detection Rule
Use missing bars, not wall-clock labels.

For a sorted OHLCV series:
- `expected_dt = interval_ms`
- `delta_i = ts[i] - ts[i-1]`
- `missing_bars_i = delta_i / expected_dt - 1`
- a gap exists when `missing_bars_i >= 1`

Use event time only:
- detect on canonical `data_ts`
- do not infer continuity from ingest order
- do not infer continuity from file boundaries

## Gap Classes
Default backtest classes:
- Small gap: `1-2` missing bars
- Medium gap: `3-7` missing bars
- Long gap / segment break: `>= 8` missing bars

Default 15m interpretation:
- Small gap: up to 30 minutes missing
- Medium gap: 45 minutes to 1 hour 45 minutes missing
- Long gap: 2 hours or more missing

Why `>= 8` for segment break:
- the current code already treats `>= 3 * interval_ms` as actionable for closed-bar lag logging
- the local 15m datasets contain repeated multi-hour holes, including `8`, `10`, `14`, `16`, `18`, and `23` missing bars
- `>= 8` is conservative enough to distinguish a brief outage from a continuity break without over-fragmenting the run

## Observed Local Gaps
The Strategy Writer should treat these as explicit known facts about the current local cleaned OHLCV datasets.

### Dataset-wide pattern
Observed under `data/cleaned/ohlcv/`:
- symbols inspected: `ADAUSDT`, `BTCUSDT`, `DOGEUSDT`, `ETHUSDT`, `SOLUSDT`, `XTZUSDT`
- intervals inspected: `15m`, `1h`, `4h`, `1d`
- span inspected: local files from `2020` through `2026`

Observed year pattern:
- most large `15m` gaps are in `2020` and `2021`
- there is a smaller `2023` gap
- no comparable large `15m` holes were observed in the local cleaned files for `2024`

### 15m summary
For `BTCUSDT`, `ADAUSDT`, `DOGEUSDT`, `ETHUSDT`, `XTZUSDT`:
- `15` detected gaps each
- histogram: `{4: 3, 5: 2, 6: 1, 8: 2, 10: 2, 14: 1, 16: 1, 18: 2, 23: 1}`
- largest gap: `23` missing bars
- largest wall-clock jump: `6.0` hours

For `SOLUSDT`:
- `10` detected gaps
- histogram: `{4: 2, 5: 2, 6: 1, 8: 1, 10: 1, 16: 1, 18: 2}`
- largest gap: `18` missing bars
- largest wall-clock jump: `4.75` hours

### 1h summary
For most non-SOL symbols:
- `15` detected gaps
- histogram pattern roughly `{1: 7, 2: 3, 3: 2, 4: 2, 5: 1}`
- largest gap: `5` missing bars

For `SOLUSDT`:
- `10` detected gaps
- largest gap: `4` missing bars

### Explicit example gaps
Representative `15m` examples the Strategy Writer should know:
- `BTCUSDT`: `2020-02-19T11:44:59.999Z -> 2020-02-19T17:44:59.999Z`, `23` missing bars
- `BTCUSDT`: `2020-03-04T09:29:59.999Z -> 2020-03-04T11:44:59.999Z`, `8` missing bars
- `BTCUSDT`: `2021-04-25T04:14:59.999Z -> 2021-04-25T08:59:59.999Z`, `18` missing bars
- `SOLUSDT`: `2020-12-21T13:59:59.999Z -> 2020-12-21T18:14:59.999Z`, `16` missing bars
- `BTCUSDT`: `2023-03-24T12:44:59.999Z -> 2023-03-24T14:14:59.999Z`, `5` missing bars

Implication:
- the current local cleaned OHLCV is not globally continuous at `15m`
- a backtest policy must distinguish recoverable short gaps from true continuity breaks
- multi-hour `15m` holes are already present locally and should not be treated as edge-case theory

## Backtest Semantics
### Small Gap
- May remain in the same episode.
- Do not synthesize the missing bars.
- Mark the gap explicitly in audit/report outputs.
- Recommended strategy/runtime behavior after implementation:
  - degrade / no-entry for the first 1-2 post-gap bars, or
  - allow hold-only until local history is clean again
- Pre-gap state may remain usable if the feature window is still locally valid and the strategy explicitly tolerates small continuity loss.

### Medium Gap
- Still no synthetic bars.
- Prefer remaining in the same high-level run, but treat the post-gap region as restricted.
- Recommended behavior after implementation:
  - no-entry / hold-only immediately after the gap
  - rewarm feature state from fresh local bars before new entries
- If a strategy depends on dense rolling windows, medium gaps should be treated closer to segment breaks.

### Long Gap / Segment Break
- Treat as an **episode boundary** or **segment break**.
- End the current continuity segment at the last pre-gap bar.
- Do not trade the gap interior.
- Restart the next segment from the first real post-gap bar only after warmup completes from local post-gap history.
- Do **not** borrow indicator, feature, or model state across the boundary.
- Do **not** treat the gap as recoverable continuity.

### Canonical Rules
- Canonical cleaned OHLCV stays unpadded.
- Gap interiors are neither traded nor synthesized.
- Long gaps are not downgraded into warnings while keeping old state alive.
- The first tradable post-gap timestamp is after segment-local warmup, not the first visible post-gap bar.

## Reporting Contract
Every backtest strategy study that relies on OHLCV should report:
- interval
- start/end timestamps
- gap count
- histogram by missing-bar count
- largest gap
- count of long-gap segment breaks under the default threshold
- representative examples with exact timestamps
- whether post-gap bars were excluded, degraded, or split into new segments

If a report compares strategies:
- use the same gap policy for all challengers
- state whether any candidate is sensitive to post-gap rewarm

## Anti-Patterns
- Padding canonical clean OHLCV as if the market were continuous.
- Linearly interpolating OHLC prices across exchange-maintenance gaps.
- Forward-filling synthetic OHLC bars without an explicit synthetic-bar contract.
- Allowing immediate entry after a long gap without rewarm.
- Carrying pre-gap indicator state across a long-gap boundary.
- Silently downgrading hard continuity failures into invisible warnings.
- Recommending a strategy off a run that ignored multi-hour 15m gaps.

## Open Implementation Hooks
Conservative future implementation hooks, if the repo later adds backtest segmentation:
- pre-scan local OHLCV files into contiguous segments before replay
- expose segment metadata to backtest artifacts
- re-run warmup at segment boundaries using only post-gap local history
- optionally apply a post-gap hold-only window for small/medium gaps

Avoid these implementation directions:
- mutating canonical parquet to fill holes
- adding live-mode semantics in the same change
- hiding segment resets inside feature code without reporting them

## Default Recommendation
For this repo, the default backtest policy should be:
- small gap: same episode, explicit degrade / no-entry handling
- medium gap: restricted post-gap mode plus local rewarm before fresh entries
- long gap (`>= 8` missing bars on 15m): hard segment break, no synthetic bars, fresh warmup from post-gap history only
