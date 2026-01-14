# Contract Specifications

This document lists the protocol interfaces enforced between layers. Each interface defines
the allowed inputs and outputs for its layer.

## Contracts
- FeatureChannel
- ModelProto
- DecisionProto
- RiskProto
- ExecutionPolicy / Router / Slippage / Matching

Cross-layer assumptions are rejected by construction.

## Code locations
- FeatureChannel: `src/quant_engine/contracts/feature.py`
- ModelProto: `src/quant_engine/contracts/model.py`
- DecisionProto: `src/quant_engine/contracts/decision.py`
- RiskProto: `src/quant_engine/contracts/risk.py`
- ExecutionPolicy: `src/quant_engine/contracts/execution/policy.py`
- Router: `src/quant_engine/contracts/execution/router.py`
- Slippage: `src/quant_engine/contracts/execution/slippage.py`
- Matching: `src/quant_engine/contracts/execution/matching.py`
