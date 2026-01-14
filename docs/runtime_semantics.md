# Runtime Semantics

## Runtime control flow: Driver-owned time
The Driver is the single time authority. The Engine is time-agnostic but time-validated:
- it never advances time
- it rejects non-monotonic time and lookahead-visible states

```mermaid
sequenceDiagram
    autonumber
    participant I as Ingestion (external)
    participant D as Driver (time authority)
    participant E as Engine (contract executor)
    participant H as DataHandlers
    participant F as FeatureExtractor
    participant M as Model
    participant R as Risk
    participant X as Execution
    participant P as Portfolio

    %% External ingestion boundary
    I-->>D: IngestionTick(data_ts, domain, symbol, payload)

    %% Bootstrap / preload (Driver chooses anchor_ts)
    D->>E: preload(anchor_ts)
    E->>H: bootstrap(anchor_ts, lookback)
    E->>H: align_to(anchor_ts)

    %% Warmup (still Driver-owned time)
    D->>E: warmup(anchor_ts)
    E->>F: initialize(anchor_ts)

    %% Runtime loop
    loop Driver-controlled time
        D->>D: choose step_ts (monotonic)
        D->>D: drain ticks <= step_ts
        D->>H: on_new_tick(tick)

        %% Hard readiness / legality gate
        D->>D: check hard readiness (OHLCV watermark >= visible_end_ts(step_ts))
        alt hard-ready
            D->>E: step(step_ts)
        else hard-missing
            D->>D: fail / block (hard violation)
        end

        %% Engine step (time-validated, contract-driven)
        E->>H: align_to(step_ts)
        E->>H: snapshot(domain,symbol) where data_ts <= step_ts

        E->>F: update(step_ts)
        E->>M: predict(features, context)

        E->>R: constrain/adjust(intent, context)
        E->>X: execute(target, primary_snapshots, step_ts)
        E->>P: apply_fills(fills, step_ts)

        E-->>D: EngineSnapshot(step_ts, diagnostics)
    end
```

## Time ownership and lookahead safety (core invariant)
SoionLab enforces a strict single-owner time model. Only the Driver (BacktestEngine, MockEngine,
RealtimeEngine) may choose when time advances, what timestamp is processed next, and how replay
is paced. All other layers are time-agnostic.

| Layer | Owns time progression? | Responsibility |
| --- | --- | --- |
| Strategy | No | Declare structure and intent only |
| Feature | No (accepts `ts` only) | Snapshot/windowed computation |
| DataHandler | No (`on_new_tick` / `align_to` only) | Cache + anti-lookahead gates |
| StrategyEngine | No (relays `ts` only) | Runtime orchestration |
| Driver / Runner | Yes | Single time authority |

Why it matters:
- Strategies never pull data.
- Features never advance time.
- DataHandlers never decide when new data arrives.
- The Engine never infers or advances timestamps.

Every timestamp used for feature computation, model prediction, risk sizing, and execution
originates from the Driver. This isolates time ownership and makes lookahead errors visible
as contract violations rather than modeling bugs.

## Runtime pipeline (bar -> snapshot -> action)
At runtime, each step executes a contract-driven pipeline:
1. DataHandlers expose market snapshots (`data_ts <= step_ts`).
2. Features update on `step_ts` and emit a feature dict.
3. Models produce scores.
4. Decision + Risk transform scores into a target position.
5. Execution produces fills (same semantics across backtest/mock/realtime).
6. Portfolio applies fills and records state.
