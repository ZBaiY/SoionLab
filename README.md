# SoionLab

[![CI](https://github.com/ZBaiY/SoionLab/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ZBaiY/SoionLab/actions/workflows/ci.yml?query=branch%3Amain)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)
![Platform](https://img.shields.io/badge/platform-Ubuntu%2022.04%20%7C%20macOS-9cf)

## Overview

SoionLab is a **contract-driven quant research & execution framework** with **one unified runtime semantics** across **Backtest / Mock (paper) / Live**.

Core idea: components communicate through explicit contracts (Protocols), while the runtime enforces **time/lifecycle correctness** and **execution realism**.

**Design rules (non-negotiable):**
- **Strategy** is a static template specification: no mode, no time, no side effects.
- Concrete symbols are resolved via an explicit **bind** step.
- **Driver** (Backtest / Mock / Realtime) is the **single time authority**.

## Support matrix

- **OS**: Ubuntu 22.04.5 LTS (x86_64), macOS (Apple Silicon)
- **Python**: 3.11, 3.12
- **CI**: GitHub Actions workflow at `.github/workflows/ci.yml`

## Installation

This repo is a **self-contained runtime instance**:
- Runtime data root: `./data/`
- Runtime artifacts root: `./artifacts/`

All filesystem paths are **repo-root anchored** (no CWD dependence).

### Quick start (VPS / Ubuntu 22.04 + Conda)

```bash
apt-get update && apt-get install -y curl bzip2
curl -fsSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -p /root/miniconda3
/root/miniconda3/bin/conda init bash
# reopen shell or: source ~/.bashrc

bash scripts/installation.sh
source /root/miniconda3/etc/profile.d/conda.sh
conda activate qe
```

### Alternative (no conda): venv

If you prefer not to use conda:
```bash
cd SoionLab
apt-get update && apt-get install -y python3-venv python3-dev build-essential
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
pip install -e .
python -c "import quant_engine, ingestion; print('imports_ok')"
```
### Testing
CI runs unit + integration tests **without local/private datasets**.
```
pytest -q -m "not local_data" tests
```
Local data tests are opt-in:
	•	mark tests with @pytest.mark.local
	•	run locally with your dataset available

### Notes

- Secrets (API keys, Telegram bot tokens, etc.) must **not** be committed. Keep them on the server (env vars or root-only files).
- Ingestion writes append-only parquet under `./data/raw/...` by design.
- **BoundStrategy** = a fully-instantiated strategy (symbols resolved).  
  This is the only form accepted by the runtime.
- **Engine** = runtime semantics (time, lifecycle, legality).
- **Driver** (BacktestEngine / RealtimeEngine) = time pusher (calls `engine.step()`), strategy-agnostic.

## Event-driven → Contract-driven
Earlier versions (tradebots legacy versions) relied on implicit control flow between components, which became fragile under multi-source data and execution constraints.

SoionLab keeps the runtime event-driven, but **logic boundaries are enforced by contracts**:
- `FeatureChannel` → features
- `ModelProto` → scores
- `DecisionProto` → intents
- `RiskProto` → target positions
- `ExecutionPolicy/Router/Slippage/Matching` → fills

## Data Ingestion (Outside Runtime)

SoionLab **does not perform data ingestion inside the runtime**.

Ingestion is an **external subsystem** responsible for:
- fetching / listening / replaying data
- normalizing raw inputs into immutable ticks
- optionally persisting data (e.g. parquet)

The runtime **never interacts with data sources**.  
It only consumes **already-normalized ticks** provided by the Driver.

**Hard boundary (single entry point):**
```
WORLD → Ingestion → Tick → Driver → Engine → DataHandler → Feature/Model
```
Key constraints:
- Ingestion may be synchronous or asynchronous and may block on I/O.
- The runtime is single-threaded and **driver-time–controlled**.
- Strategy / Engine / DataHandler never know data provenance.
- The only object crossing the boundary is an immutable `IngestionTick`.

---

## Strategy Structure vs Runtime Execution

SoionLab strictly separates **strategy structure** from **runtime execution**.

### 1. Strategy (template, static)

A Strategy declares:
- data semantics (domains, intervals, lookbacks)
- feature topology
- model / decision / risk / execution structure

It may use symbolic placeholders (e.g. `{A}`, `{B}`), but it contains:
- no time semantics
- no runtime state
- no data access

### 2. Bind step (structural only)

The bind step resolves placeholders into a concrete universe
(primary / secondary symbols).

This step:
- is deterministic
- introduces no time, data, or execution semantics
- produces a fully-specified **BoundStrategy**

### 3. StrategyLoader (assembly, no time)

`StrategyLoader` consumes a BoundStrategy and assembles:
- DataHandlers (empty, cache-only shells)
- FeatureExtractor
- Models
- Risk rules
- Execution pipeline
- StrategyEngine

At the end of this phase:
- the Engine exists
- no data has been seen
- no time has advanced

---

## Runtime Control Flow (Driver-Owned Time)

The Driver is the **sole time authority**.

The Engine is time-agnostic but **time-validated**:
- it never advances time
- it rejects non-monotonic or lookahead states

```mermaid
sequenceDiagram
    autonumber
    participant I as Ingestion
    participant D as Driver
    participant E as StrategyEngine
    participant H as DataHandlers
    participant F as FeatureExtractor
    participant M as Model
    participant R as Risk
    participant X as Execution
    participant P as Portfolio

    %% Ingestion is external
    I-->>D: IngestionTick(ts, domain, symbol)

    %% Bootstrap / warmup
    D->>E: preload_data(anchor_ts)
    E->>H: bootstrap(anchor_ts, lookback)

    D->>E: warmup_features(anchor_ts)
    E->>H: align_to(anchor_ts)
    E->>F: initialize(anchor_ts)

    %% Runtime loop
    loop Driver-controlled time
        D->>H: on_new_tick(tick)
        D->>E: step(ts)

        E->>H: align_to(ts)
        E->>H: collect snapshots (per-domain, per-symbol)

        E->>F: update(ts)
        E->>M: predict(features, context)
        E->>R: adjust(intent, context)
        E->>X: execute(target, primary_snapshots, ts)
        E->>P: apply fills

        E-->>D: EngineSnapshot(ts)
    end
```

# Time Ownership & Lookahead Safety (Core Invariant)

## Single Source of Time Truth

SoionLab enforces a **strict single-owner time model**.

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

In SoionLab:
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

# How a Market Bar Flows Through the SoionLab
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
This JSON **only describes runtime assembly (data semantics, features, models)**, and one can save the settings inside the strategy module.

**Data ingestion configuration is intentionally excluded and lives outside the runtime**.

Following is the *runtime assembly config* consumed by `StrategyLoader.from_config(...)`. In practice your real strategies will have more features and data sources; the important part is the **shape** (and the naming convention).

```json
{
  "data": {
    "primary": {
      "ohlcv":        { "$ref": "OHLCV_1M_30D" },
      "trades":       { "$ref": "TRADES_1S" },
      "option_trades":{ "$ref": "OPTION_TRADES_RAW" },
      "iv_surface":   { "$ref": "IV_SURFACE_5M" },
      "sentiment":    { "$ref": "SENTIMENT_BASIC_5M" }
    },
    "secondary": {
      "{B}": {
        "ohlcv": { "$ref": "OHLCV_1M_30D" }
      }
    }
  },

  "features_user": [
    {
      "name": "SPREAD_MODEL_{A}^{B}",
      "type": "SPREAD",
      "symbol": "{A}",
      "params": { "ref": "{B}" }
    },
    {
      "name": "ZSCORE_MODEL_{A}^{B}",
      "type": "ZSCORE",
      "symbol": "{A}",
      "params": { "ref": "{B}", "lookback": 120 }
    },
    {
      "name": "ATR_RISK_{A}",
      "type": "ATR",
      "symbol": "{A}",
      "params": { "window": 14 }
    }
  ],

  "model": {
    "type": "PAIR_ZSCORE",
    "params": {
      "zscore_feature": "ZSCORE_MODEL_{A}^{B}"
    }
  },

  "decision": { "...": "omitted" },
  "risk":     { "...": "omitted" },
  "execution":{ "...": "omitted" },
  "portfolio":{ "...": "omitted" }
}
```
This config does NOT specify data sources, transports, or persistence. Those belong to the ingestion layer.

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

# 3) assembly: BoundStrategy -> StrategyEngine (no time, no ingestion)
engine = StrategyLoader.from_config(
    strategy=strategy,
    mode=EngineMode.BACKTEST,
)

# 4) driver: owns time; ingestion runs externally
# Ingestion is assumed to be running externally and feeding ticks into handlers
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
It enables the SoionLab to gracefully support:
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
    TRD[Trades<br>Aggregated / Tick-level]
    MKT[Market Data<br>Binance Klines<br>]
    OPT[Derivatives Data<br>Option Trades<br>executions]
    ALT[Alternative Data<br>News<br>Twitter X<br>Reddit] 
end

subgraph L1[Layer 1 — Data Ingestion]
    TRDH[TradesHandler<br>stream trades<br>update windows]
    RTDH[RealTimeDataHandler<br>stream bars<br>update windows]
    OTDH[OptionTradesHandler<br>append-only<br>execution facts]
    SLOAD[SentimentLoader<br>fetch news tweets<br>cache dedupe]
end

TRD --> TRDH
MKT --> RTDH
OPT --> OTDH
ALT --> SLOAD

subgraph L2[Layer 2 — Feature Layer]
    FE[FeatureExtractor<br>TA indicators<br>Microstructure<br>Vol indicators<br>IV factors]
    IVFEAT[IVSurfaceFeature<br>ATM IV<br>Skew/Smile<br>Term Structure<br>Vol-of-vol<br>Roll-down]
    SENTPIPE[SentimentPipeline<br>text cleaning<br>FinBERT VADER fusion<br>sentiment score vol velocity]
    MERGE[Merge Features<br>TA + microstructure + vol + IV + sentiment<br>kept as a dict handled by strat]
end

TRDH --> FE
RTDH --> FE
OTDH --> IVFEAT
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
