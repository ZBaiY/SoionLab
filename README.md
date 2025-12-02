# Quant_Engine

<p align="center" style="font-size:26px; font-weight:700; margin-top:20px;">
  The Quant Engine (TradeBot v4) evolves from a monolithic pipeline<br>
  to a more modular, more scalable quant research + execution platform.<br>
  This is the foundation required for professional-grade systematic trading.
</p>

------------------------

## Key transition from Event-Driven to Contract-Driven Architecture
Early versions of the trading engine (v2–v3) followed a purely event-driven design, that is whenever a new market bar arrived, the system sequentially executed a fixed pipeline:
```
Data → Features → Model → Signal → Strategy → Risk → Order → Execution
```

This design worked for small, single-strategy prototypes,
but as the system expanded—adding sentiment signals, IV surface models,
multiple strategies, execution policies, and realistic slippage modelling—
the architecture became increasingly fragile and difficult to extend:
	•	Feature extraction contained strategy-specific logic
	•	Strategies directly manipulated risk and execution components
	•	Backtest and live trading followed different execution semantics
	•	New models required modifying core pipeline code
	•	Slippage and routing logic leaked into strategy implementation
	•	The system could not scale to multi-asset or regime-based research

To overcome these limitations, Quant Engine (Tradebot v4) adopts a Contract-Driven Architecture.

------------------------

## What Contract-Driven Means

In v4, each logical layer exposes a formal Protocol (contract) describing what it provides,
while hiding how it is implemented.
Components communicate only through these contracts:
	•	ModelProto — transforms feature data into continuous scores
	•	DecisionProto — converts model scores into trade intents
	•	RiskProto — determines position sizing, stops, constraints
	•	ExecutionPolicy — converts target positions into child orders
	•	Router / SlippageModel / MatchingEngine — provide realistic execution semantics
	•	FeatureChannel — modular feature streams (TA, microstructure, sentiment, IV, cross-asset)

Runtime remains event-driven (each new market bar triggers the pipeline),
but the internal logic is now contract-driven, producing a clean separation of responsibilities.

------------------------

## Benefits of the v4 Architecture
While earlier versions of the engine already had basic modularity
(e.g., switching models or risk managers through JSON configuration),
v4 introduces a deeper, more systematic form of modularity based on explicit contracts and strict separation of concerns.

This enables the engine to evolve from a prototype pipeline into a scalable research and execution infrastructure.


1. More Modular (Contract-Level Modularity)
Components no longer depend on each other’s internal implementations.
Models, decision rules, risk engines, and execution policies can be replaced independently through contract interfaces,
not through code branches.

3.  More Composable (Strategies as Configurations)
A strategy is now built by composing contract-defined components:
```
model → decision → risk → execution
```
Instead of writing new strategy classes, users simply assemble components in configuration.

4. More Extensible (Feature Channels & Plug-in Modules)
New feature channels—sentiment, IV surface, microstructure, volatility regimes—plug in immediately
without touching strategy, model, or execution code.

5. More Execution-Realistic (Unified Execution Layer)
Backtest and live trading now share the exact same execution semantics:

	•	slippage modelling
	•	routing logic
	•	partial fills
	•	maker/taker fees
	•	integer rounding rules

Execution realism becomes intrinsic to the engine, not added on top.

6. More Scalable (Multi-Asset, Multi-Strategy, ML-Driven)

Because each layer is contract-isolated, the system naturally supports:

	•	multi-asset trading
	•	cross-sectional models
	•	ensemble and ML models
	•	regime-aware strategies
	•	multi-strategy portfolios

All without modifying the pipeline or touching other components.

------------------------

## Why This Matters

This architectural shift enables the Quant Engine to gracefully support:

	•	ML-based sentiment regimes
	•	Microstructure-aware execution
	•	IV-surface-derived features (SABR / SSVI)
	•	Volatility forecasting modules
	•	Multi-asset and cross-asset trading
	•	Backtest-live reproducibility
	•	Research and execution decoupled but interoperable
	


------------------------

```mermaid
flowchart TD

%% ==========================================================
%% LAYER 0 — DATA SOURCES
%% ==========================================================

subgraph L0[Layer 0 — Data Sources]
    MKT[Market Data<br>Binance Klines<br>Orderbook L1 L2<br>Trades]
    ALT[Alternative Data<br>News<br>Twitter X<br>Reddit]
    OPT[Derivatives Data<br>Option Chains<br>IV Surface<br>OU Model]
end


%% ==========================================================
%% LAYER 1 — DATA INGESTION
%% ==========================================================

subgraph L1[Layer 1 — Data Ingestion]
    HDH[HistoricalDataHandler<br>clean check rescale]
    RTDH[RealTimeDataHandler<br>stream bars<br>update windows]
    SLOAD[SentimentLoader<br>fetch news tweets<br>cache dedupe]
end

MKT --> HDH
MKT --> RTDH
ALT --> SLOAD
OPT --> HDH


%% ==========================================================
%% LAYER 2 — FEATURE LAYER
%% ==========================================================

subgraph L2[Layer 2 — Feature Layer]
    FE[FeatureExtractor<br>TA indicators<br>Microstructure<br>Vol indicators<br>IV factors]
    SENTPIPE[SentimentPipeline<br>text cleaning<br>FinBERT VADER fusion<br>sentiment score vol velocity]
    MERGE[Merge Features<br>TA + microstructure + vol + sentiment]
end

HDH --> FE
RTDH --> FE
SLOAD --> SENTPIPE
FE --> MERGE
SENTPIPE --> MERGE


%% ==========================================================
%% LAYER 3 — MODELING LAYER
%% ==========================================================

subgraph L3[Layer 3 — Modeling Layer ModelProto]
    MODEL[Model Library<br>Statistical<br>ML models<br>Regime classifier<br>Physics OU models]
end

MERGE --> MODEL


%% ==========================================================
%% LAYER 4 — DECISION LAYER
%% ==========================================================

subgraph L4[Layer 4 — Decision Layer DecisionProto]
    DECIDE[Decision Engine<br>Signal + sentiment regime fusion<br>Threshold gating]
end

MODEL --> DECIDE
SENTPIPE --> DECIDE


%% ==========================================================
%% LAYER 5 — RISK LAYER
%% ==========================================================

subgraph L5[Layer 5 — Risk Layer RiskProto]
    RISK[Risk Engine<br>SL TP<br>ATR volatility<br>Sentiment scaled size<br>Portfolio exposure]
end

DECIDE --> RISK
SENTPIPE --> RISK


%% ==========================================================
%% LAYER 6 — EXECUTION LAYER
%% ==========================================================

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


%% ==========================================================
%% LAYER 7 — PORTFOLIO UPDATE
%% ==========================================================

subgraph L7[Portfolio and Accounting]
    PORT[Portfolio Manager<br>positions<br>PnL<br>leverage<br>exposures]
end

MATCH --> PORT


%% ==========================================================
%% LAYER 8 — REPORTING
%% ==========================================================

subgraph L8[Reporting Engine]
    REPORT[Reporting<br>Backtest metrics<br>IS Slippage<br>Factor exposure<br>Sentiment regime attribution]
end

PORT --> REPORT
DECIDE --> REPORT
RISK --> REPORT
SENTPIPE --> REPORT
