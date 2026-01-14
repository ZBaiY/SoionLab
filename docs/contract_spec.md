# Contract Spec (Overview)

SoionLab enforces explicit interfaces between layers. Components communicate only through
their contracts; cross-layer assumptions are rejected by construction.

## Interfaces and code locations
- FeatureChannel: `src/quant_engine/contracts/feature.py`
- ModelProto: `src/quant_engine/contracts/model.py`
- DecisionProto: `src/quant_engine/contracts/decision.py`
- RiskProto: `src/quant_engine/contracts/risk.py`
- ExecutionPolicy: `src/quant_engine/contracts/execution/policy.py`
- Router: `src/quant_engine/contracts/execution/router.py`
- Slippage: `src/quant_engine/contracts/execution/slippage.py`
- Matching: `src/quant_engine/contracts/execution/matching.py`

Each interface defines the allowed inputs/outputs for its layer; implementations are wired
by the strategy loader and executed by the runtime without bypass paths.
