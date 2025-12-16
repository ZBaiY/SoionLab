<h1 align="center">
  <strong>
    The Quant Engine (TradeBot v4)
  </strong>
</h1>

<p align="center" style="font-size:26px; font-weight:600; line-height:1.35; padding:10px 0;">
  A modular, extensible, execution-realistic research & trading framework —
  designed for professional-grade systematic trading.
</p>

---

# Overview
Quant Engine (TradeBot v4) is a **contract-driven quant research & execution framework** with **one unified runtime semantics** across:
- **Backtest**
- **Mock (paper) trading**
- **Live trading**

Core idea: components communicate through explicit contracts (Protocols), while the runtime enforces **time/lifecycle correctness** and **execution realism**.

**Design rules (non-negotiable):**
- **Strategy** = static specification (what to run). No mode, no time, no side effects.
- **Engine** = runtime semantics (time, lifecycle, legality).
- **Driver** (BacktestEngine / RealtimeEngine) = time pusher (calls `engine.step()`), strategy-agnostic.

## Event-driven → Contract-driven
Earlier versions chained logic directly (Data → Features → Model → Decision → Risk → Execution), which became fragile with multi-source data and execution realism.

v4 keeps the runtime event-driven, but **logic boundaries are enforced by contracts**:
- `FeatureChannel` → features
- `ModelProto` → scores
- `DecisionProto` → intents
- `RiskProto` → target positions
- `ExecutionPolicy/Router/Slippage/Matching` → fills

## Strategy loading and runtime control-flow

```mermaid
sequenceDiagram
    autonumber
    participant U as User / Entry
    participant S as Strategy (static spec)
    participant L as StrategyLoader
    participant E as StrategyEngine (runtime semantics)
    participant D as Driver (BacktestEngine / RealtimeEngine)
    participant H as DataHandlers (OHLCV/Orderbook/Options/IV/Sentiment)
    participant F as FeatureExtractor
    participant M as Model
    participant R as Risk
    participant X as ExecutionEngine
    participant P as Portfolio

    U->>S: strategy = ExampleStrategy()
    U->>L: from_config(strategy, mode, overrides?)

    Note over L: DATA is the *only* symbol declaration source
    L->>H: build_multi_symbol_handlers(data_spec, backtest?, primary_symbol)
    Note over H: handlers are created as shells (no history loaded yet)

    L->>F: FeatureLoader.from_config(features_user, handlers...)
    L->>M: build_model(type, symbol, **params)
    L->>R: RiskLoader.from_config(risk_cfg, symbol?)
    L->>E: assemble StrategyEngine(mode, handlers, F, M/R/Decision, execution, portfolio)

    Note over E: Engine is assembled but not running yet
    D->>E: (BACKTEST only) load_history(start_ts, end_ts)
    E->>H: handler.load_history(start_ts, end_ts)

    D->>E: warmup(anchor_ts, warmup_steps)
    E->>H: warmup_to(anchor_ts) / align cursors
    loop warmup steps
        E->>F: update(anchor_ts)
    end

    loop main loop (Driver-controlled)
        D->>E: step()
        E->>H: pull market snapshot (primary clock)
        E->>F: update(timestamp)
        E->>M: predict(features)
        E->>R: adjust(intent, context)
        E->>X: execute(target, market_data, timestamp)
        E->>P: apply fills / update state
        E-->>D: snapshot{timestamp, features, scores, target, fills, portfolio}
    end
```

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
```json
{
  "BTCUSDT": {
    "strategy": {
      "model": {
        "class": "RSIModel",
        "params": { "window": 14 }
      },
      "decision": {
        "class": "ThresholdDecision",
        "params": { "threshold": 0.0 }
      },
      "risk": {
        "class": "ATRSizer",
        "params": { "atr_window": 14, "risk_fraction": 0.02 }
      },
      "execution": {
        "class": "TWAPPolicy",
        "params": { "segments": 5 }
      }
    }
  }
}
```
This JSON assembles components — it does **not** select branches inside a pipeline.

---

# Minimal Working Example (Python)
```python
from quant_engine import (
    RSIModel,
    ThresholdDecision,
    ATRSizer,
    TWAPPolicy,
    StrategyEngine,
)

strategy = StrategyEngine(
    model=RSIModel(window=14),
    decision=ThresholdDecision(threshold=0.0),
    risk=ATRSizer(atr_window=14, risk_fraction=0.02),
    execution=TWAPPolicy(segments=5),
)

strategy.backtest(
    symbol="BTCUSDT",
    start="2022-01-01",
    end="2023-01-01"
)

strategy.report.save("reports/btc_rsi_twap/")
```

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
