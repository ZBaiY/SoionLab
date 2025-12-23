# Overview
Quant Engine (TradeBot v4) is a **contract-driven quant research & execution framework** with **one unified runtime semantics** across:
- **Backtest**
- **Mock (paper) trading**
- **Live trading**

Core idea: components communicate through explicit contracts (Protocols), while the runtime enforces **time/lifecycle correctness** and **execution realism**.

**Design rules (non-negotiable):**
- **Strategy** = static *template* specification (what to run).  
  No mode, no time, no side effects.  
  Concrete symbols are resolved via an explicit **bind** step.
- **BoundStrategy** = a fully-instantiated strategy (symbols resolved).  
  This is the only form accepted by the runtime.
- **Engine** = runtime semantics (time, lifecycle, legality).
- **Driver** (BacktestEngine / RealtimeEngine) = time pusher (calls `engine.step()`), strategy-agnostic.

## Event-driven → Contract-driven
Earlier versions relied on implicit control flow between components, which became fragile under multi-source data and execution constraints.

v4 keeps the runtime event-driven, but **logic boundaries are enforced by contracts**:
- `FeatureChannel` → features
- `ModelProto` → scores
- `DecisionProto` → intents
- `RiskProto` → target positions
- `ExecutionPolicy/Router/Slippage/Matching` → fills

## Data Ingestion (Outside Runtime)

Quant Engine v4 **does not include data ingestion in the runtime**.

Ingestion is an **external subsystem** responsible for:
- fetching / listening / replaying data
- normalizing it into ticks
- optionally persisting data (e.g. parquet)

The runtime **never pulls data**.  
It only consumes **already-arrived ticks**.

**Hard boundary:**
```
Ingestion → Tick → Driver → Engine → DataHandler
```
- Ingestion may run in parallel and block on I/O.
- Runtime is single-threaded and time-driven.
- Strategy / Engine / DataHandler never know data sources.

Strategy configs describe **data semantics**, not data provenance.

## Strategy loading and runtime control-flow

### Strategy Template → Bind → Runtime

Quant Engine v4 distinguishes **strategy structure** from **strategy instantiation**:

1. **Strategy (template)**  
   Declares data dependencies, feature structure, and model logic using symbolic placeholders  
   (e.g. `{A}`, `{B}` for asset roles).

2. **Bind step**  
   Resolves placeholders into a concrete *universe* (primary / secondary symbols).  
   This step is purely structural and introduces **no runtime or time semantics**.

3. **BoundStrategy**  
   The fully-resolved strategy specification consumed by `StrategyLoader`.

This separation enables clean research semantics, explicit symbol universes, and reproducible execution.

```mermaid
sequenceDiagram
    autonumber
    participant U as User / Entry
    participant S as Strategy (static template)
    participant B as BoundStrategy
    participant L as StrategyLoader
    participant D as Driver / Runner
    participant E as StrategyEngine (time-agnostic)
    participant H as DataHandlers (runtime only)
    participant F as FeatureExtractor
    participant M as Model
    participant R as Risk
    participant X as ExecutionEngine
    participant P as Portfolio
    %% -------------------------------
    %% Assembly phase (no time)
    %% -------------------------------
    U->>S: strategy_tpl = ExampleStrategy()
    U->>B: strategy = strategy_tpl.bind(A, B)
    U->>L: from_config(strategy, mode)
    L->>H: build handlers (shells, empty cache)
    L->>F: build FeatureExtractor
    L->>M: build models
    L->>R: build risk rules
    L->>E: assemble StrategyEngine

    Note over E: Engine assembled (no data, no time)

    %% -------------------------------
    %% Bootstrap / warmup (Driver owns time)
    %% -------------------------------
    D->>E: preload_data(anchor_ts)
    E->>H: handler.bootstrap(anchor_ts, lookback)

    D->>E: warmup_features(anchor_ts)
    E->>H: handler.align_to(anchor_ts)
    E->>F: warmup(anchor_ts)

    %% -------------------------------
    %% Runtime loop (single time authority)
    %% -------------------------------
    loop Driver-controlled time loop
        D->>H: on_new_tick(data @ ts)
        D->>E: step(ts)

        E->>H: align_to(ts)
        E->>F: update(ts)
        E->>M: predict(features)
        E->>R: adjust(intent, context)
        E->>X: execute(target, market_data, ts)
        E->>P: apply fills

        E-->>D: snapshot(ts, features, target, fills, portfolio)
    end
```

# Time Ownership & Lookahead Safety (v4 Core Invariant)

## Single Source of Time Truth

Quant Engine v4 enforces a **strict single-owner time model**.

Only the **Driver / Runner** (BacktestEngine, RealtimeEngine, MockEngine) is allowed to:
- decide *when* time advances
- decide *which timestamp* is processed next
- control replay speed, ordering, and stopping conditions

All other layers are **time-agnostic**.

| Layer | Knows how time advances? | Responsibility |
|------|--------------------------|----------------|
| Strategy | ❌ | Declare structure and intent only |
| Feature | ❌ (accepts timestamp only) | Snapshot / windowed computation |
| DataHandler | ❌ (on_new_tick / align_to only) | Cache + anti-lookahead |
| StrategyEngine | ❌ (timestamp relay only) | Runtime orchestration |
| **Driver / Runner** | ✅ **Yes** | **Single time authority** |

## Why This Matters: Lookahead Safety

Lookahead bias is not a modeling bug — it is a **time ownership bug**.

In v4:
- No Strategy can pull data
- No Feature can advance time
- No DataHandler can decide *when* new data arrives
- The Engine never infers, guesses, or advances timestamps

Every timestamp used in:
- feature computation
- model prediction
- risk sizing
- execution simulation

originates **exclusively** from the Driver.

This guarantees:
- deterministic backtests
- identical execution semantics across backtest / mock / live
- zero accidental future data access

## Backtest, Mock, Live: Same Engine, Different Drivers

The StrategyEngine is identical across all modes.

What changes is only the Driver:
- BacktestEngine: replays historical ticks
- MockEngine: advances synthetic or delayed real data
- RealtimeEngine: advances wall-clock driven ingestion

Because time ownership is isolated:
- switching modes requires **no strategy changes**
- execution realism is preserved
- research results transfer cleanly to production

> **Invariant:**  
> If a component does not own time, it must never decide or infer time.

---

# How a Market Bar Flows Through the Quant Engine (v4)
At runtime, each new market bar triggers a clean, contract-driven pipeline:

1. Handlers provide the current market snapshot (multi-source)
2. Features are computed and merged into a single feature dict
3. Models output scores
4. Decision + Risk convert scores into a target position
5. Execution layer produces fills (same semantics across backtest/mock/live)
6. Portfolio + reporting update P&L / accounting / traces

Each layer depends **only on contracts**, not implementations.

---

# Minimal Strategy Configuration Example (v4 JSON)
This is the *runtime assembly config* consumed by `StrategyLoader.from_config(...)`. In practice your real strategies will have more features and data sources; the important part is the **shape** (and the naming convention).

```json
{
  "data": {
    "primary": {
      "ohlcv": { "symbol": "BTCUSDT", "tf": "15m" },
      "orderbook": { "symbol": "BTCUSDT", "depth": 20 }
    },
    "secondary": {
      "ETHUSDT": {
        "ohlcv": { "tf": "15m" }
      }
    }
  },
  "features_user": [
    { "name": "RSI_MODEL_BTCUSDT", "type": "RSI", "symbol": "BTCUSDT", "params": { "window": 14 } },
    { "name": "ATR_RISK_BTCUSDT", "type": "ATR", "symbol": "BTCUSDT", "params": { "window": 14 } }
  ],
  "model": {
    "type": "RSI_MODEL",
    "params": { "rsi_feature": "RSI_MODEL_BTCUSDT" }
  },
  "decision": {
    "type": "THRESHOLD",
    "params": { "threshold": 0.0 }
  },
  "risk": {
    "type": "ATR_SIZER",
    "params": { "risk_fraction": 0.02 }
  },
  "execution": {
    "type": "TWAP",
    "params": { "segments": 5 }
  }
}
```

Notes:
- **Symbols are declared only in `data`** (primary + secondary). Features/models may reference symbols but must not introduce new ones.
- Feature names follow: `TYPE_PURPOSE_SYMBOL` (and if there is a ref: `TYPE_PURPOSE_REF^SYMBOL`).


---

# Minimal Working Example (Python)
### Example 1 — Pair Strategy (A + B)
```python
from quant_engine.strategy.engine import EngineMode
from quant_engine.strategy.loader import StrategyLoader
from quant_engine.backtest.engine import BacktestEngine

from strategies.example_strategy import ExampleStrategy

# 1) define strategy template
strategy_tpl = ExampleStrategy()

# 2) bind concrete universe (no time semantics)
strategy = strategy_tpl.bind(
    A="BTCUSDT",
    B="ETHUSDT",
)

# 3) assembly: BoundStrategy + mode -> StrategyEngine
engine = StrategyLoader.from_config(
    strategy=strategy,
    mode=EngineMode.BACKTEST,
)

# 4) driver: owns time and execution horizon
BacktestEngine(
    engine=engine,
    start_ts=1640995200.0,   # 2022-01-01 UTC
    end_ts=1672531200.0,     # 2023-01-01 UTC
    warmup_steps=200,
).run()
```
### Example 2 — Single-Asset Strategy (A only, no B)
Some strategies operate on a single asset and do not require a secondary symbol.
This is a valid **B-style degenerate case**, where only `{A}` is bound.
```python
from quant_engine.strategy.engine import EngineMode
from quant_engine.strategy.loader import StrategyLoader
from quant_engine.backtest.engine import BacktestEngine
from strategies.rsi_adx_sideways import RSIADXSidewaysStrategy
# 1) define strategy template (single-asset)
strategy_tpl = RSIADXSidewaysStrategy()
# 2) bind only the primary asset
strategy = strategy_tpl.bind(
    A="BTCUSDT",
)
# 3) assembly: BoundStrategy + mode -> StrategyEngine
engine = StrategyLoader.from_config(
    strategy=strategy,
    mode=EngineMode.BACKTEST,
)
# 4) driver: owns time and execution horizon
BacktestEngine(
    engine=engine,
    start_ts=1640995200.0,
    end_ts=1672531200.0,
    warmup_steps=50,
).run()
```
Notes:
- `{A}`-only binding is a first-class use case.
- Pair / multi-asset strategies simply extend this pattern by introducing `{B}`, `{C}`, etc.
- Strategy structure remains static; only the bound universe changes.
- Time ranges (`start_ts`, `end_ts`, `warmup_steps`) are **driver concerns only**.
- `EngineSpec` carries runtime semantics (mode, primary symbol, universe) but never time.
- Backtest, mock, and live trading share identical execution semantics.
---

# Why This Architectural Shift Matters
It enables the Quant Engine to gracefully support:
- ML-based sentiment regimes
- microstructure-aware execution
- IV-surface-derived features (SABR / SSVI)
- volatility forecasting
- multi-asset & cross-asset strategies
- execution-realistic mock trading
- reproducible backtests with live parity
- research & execution decoupled but interoperable

---

# Full System Architecture Diagram
```mermaid
flowchart TD

subgraph L0[Layer 0 — Data Sources]
    OBD[Orderbook L1 L2<br>Trades]
    MKT[Market Data<br>Binance Klines<br>]
    OPT[Derivatives Data<br>Option Chains<br>raw bid/ask/strike/expiry]
    ALT[Alternative Data<br>News<br>Twitter X<br>Reddit] 
end

subgraph L1[Layer 1 — Data Ingestion]
    ROBD[RealTimeOrderbookHandler<br>stream bars<br>update windows]
    RTDH[RealTimeDataHandler<br>stream bars<br>update windows]
    OCDH[OptionChainDataHandler<br>group by expiry<br>cache chains]
    SLOAD[SentimentLoader<br>fetch news tweets<br>cache dedupe]
end

OBD --> ROBD
MKT --> RTDH
OPT --> OCDH
ALT --> SLOAD

subgraph L2[Layer 2 — Feature Layer]
    FE[FeatureExtractor<br>TA indicators<br>Microstructure<br>Vol indicators<br>IV factors]
    IVFEAT[IVSurfaceFeature<br>ATM IV<br>Skew/Smile<br>Term Structure<br>Vol-of-vol<br>Roll-down]
    SENTPIPE[SentimentPipeline<br>text cleaning<br>FinBERT VADER fusion<br>sentiment score vol velocity]
    MERGE[Merge Features<br>TA + microstructure + vol + IV + sentiment<br>kept as a dict handled by strat]
end

ROBD --> FE
RTDH --> FE
RTDH --> IVFEAT
OCDH --> IVFEAT
SLOAD --> SENTPIPE

FE --> MERGE
IVFEAT --> MERGE
SENTPIPE --> MERGE

subgraph L3[Layer 3 — Modeling Layer ModelProto]
    MODEL[Model Library<br>Statistical<br>ML models<br>Regime classifier<br>Physics OU models]
end

MERGE --> MODEL

subgraph L4[Layer 4 — Decision Layer DecisionProto]
    DECIDE[Decision Engine<br>Signal + sentiment regime fusion<br>Threshold gating]
end

MODEL --> DECIDE
MERGE --> DECIDE

subgraph L5[Layer 5 — Risk Layer RiskProto]
    RISK[Risk Engine<br>SL TP<br>ATR volatility<br>Sentiment scaled size<br>Portfolio exposure]
end

MERGE --> RISK

subgraph L6[Layer 6 — Execution Layer]
    direction LR
    POLICY[ExecutionPolicy<br>Immediate<br>TWAP<br>Maker first]
    ROUTER[Router<br>L1 L2 aware<br>timeout rules]
    SLIP[SlippageModel<br>Linear impact<br>Depth model]
    MATCH[MatchingEngine<br>Backtest = Live]
end

RISK --> POLICY
POLICY --> ROUTER
ROUTER --> SLIP
SLIP --> MATCH

subgraph L7[Portfolio and Accounting]
    PORT[Portfolio Manager<br>positions<br>PnL<br>leverage<br>exposures]
end

MATCH --> PORT

subgraph L8[Reporting Engine]
    REPORT[Reporting<br>Backtest metrics<br>IS Slippage<br>Factor exposure<br>Sentiment regime attribution]
end

PORT --> REPORT
DECIDE --> REPORT
DECIDE --> RISK
RISK --> REPORT
SENTPIPE --> REPORT
IVFEAT --> REPORT
```
