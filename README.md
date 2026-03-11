# SoionLab

[![CI](https://github.com/ZBaiY/SoionLab/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ZBaiY/SoionLab/actions/workflows/ci.yml?query=branch%3Amain)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)
![Platform](https://img.shields.io/badge/platform-Ubuntu%2022.04%20%7C%20macOS-9cf)

## What is SoionLab
SoionLab is a **research-first trading engine** built for **fast strategy research**, **structured AI-agent workflows**, and **strict research semantics**. It is designed to let ideas move quickly from specification to experiment, produce auditable research artifacts with low friction, and then push selected results into a **live-probe environment** for limited real-world validation.

The system is intentionally optimized for the research loop: define contracts clearly, iterate on strategies quickly, generate backtest and analysis outputs systematically, and preserve semantic discipline across backtest, mock, and realtime modes. Rather than presenting live trading as the primary objective, SoionLab treats live execution as an extension of research — a controlled environment for testing how research results behave under non-ideal runtime conditions.

In practice, SoionLab emphasizes four things:
- rapid strategy iteration,
- AI-assisted and contract-driven development,
- strict control of time and data semantics in research runs,
- low-friction promotion of research outputs into live probe workflows.

Core research question: what is the robustness boundary of a strategy when it moves from semantically controlled research conditions into non-ideal live environments with heterogeneous timing, ordering, and data-readiness constraints?

## What is special: auditable execution risk
- Research-first runtime with explicit boundaries between strategy definition, data readiness, and execution.
- Structured AI-agent workflow enabled by clear contracts, composable interfaces, and low-friction strategy wiring.
- Single time authority / driver-owned time to prevent lookahead-by-construction in research semantics.
- Auditable live-probe failure surface: Hard Readiness vs Soft Degradation under asynchronous multi-source arrival.

**Intended research domains**  
SoionLab explicitly targets challenging domains such as option chains and sentiment as *execution and timing stressors*, rather than as finished modeling features.  
These domains update asynchronously, lack closed-bar semantics, and may arrive incomplete. The current focus is on making their readiness, staleness, and absence explicit in the runtime, not on pricing or alpha extraction.


## 3-Min Quick Start
```bash
bash scripts/installation.sh
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate qe
```

Run sample backtest:
```bash
python apps/run_sample.py
```
Uses bundled data under `data/sample/` for demonstration only; intended to validate wiring + trace/log emission, not PnL. See [`docs/sample_data.md`](docs/sample_data.md) for scope and limitations.

## Live Modes
Run realtime mode (default strategy wiring):
```bash
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate qe
python apps/run_realtime.py
```

### How To Run Safely
1. Set environment variables before starting live mode.
2. Run exactly one app entrypoint with explicit CLI args.
3. Let the app own lifecycle/shutdown (`Ctrl+C` triggers cooperative stop).

Backtest (explicit strategy/symbol window):
```bash
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate qe
python apps/run_backtest.py \
  --strategy EXAMPLE \
  --symbols A=BTCUSDT,B=ETHUSDT \
  --start-ts 1764374400000 \
  --end-ts 1767063600000 \
  --data-root data
```

Realtime setup helper:
```bash
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate qe
source scripts/qe_env.sh testnet
```

The `qe` activation hook installed by `scripts/installation.sh` already sources `scripts/qe_env.sh` automatically. Re-sourcing it with `testnet` or `mainnet` is useful when you want to switch profiles in the current shell.

Realtime (testnet with preflight validation):
```bash
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate qe
source scripts/qe_env.sh testnet
export BINANCE_TESTNET_API_KEY="<your_testnet_api_key>"
export BINANCE_TESTNET_API_SECRET="<your_testnet_api_secret>"
python apps/run_realtime.py \
  --strategy EXAMPLE \
  --symbols A=BTCUSDT,B=ETHUSDT \
  --binance-env testnet
```

Mainnet safeguard:
- `BINANCE_ENV=mainnet` requires `BINANCE_MAINNET_CONFIRM=YES` at startup.
- `BINANCE_BASE_URL` overrides are checked against the selected profile to fail fast on testnet/mainnet mismatches.
- mainnet startup is blocked on non-flat inventory unless `LIVE_ALLOW_NONFLAT_START=YES` is explicitly set.

Entry-point CLI summary:
- `apps/run_backtest.py`: `--strategy|--strategy-config`, `--symbols`, `--start-ts`, `--end-ts`, `--data-root`, `--run-id`
- `apps/run_sample.py`: `--strategy|--strategy-config`, `--symbols`, `--start-ts`, `--end-ts`, `--data-root`, `--run-id`
- `apps/run_realtime.py`: `--strategy|--strategy-config`, `--symbols`, `--run-id`, `--binance-env`, `--binance-base-url`, `--deribit-base-url`
- `apps/run_mock.py`: `--strategy|--strategy-config`, `--symbols`, `--timestamps`, `--run-id`

Run live Binance mode on testnet (only when your strategy `matching.type` is `LIVE-BINANCE`):
```bash
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate qe
source scripts/qe_env.sh testnet
export BINANCE_TESTNET_API_KEY="<your_testnet_api_key>"
export BINANCE_TESTNET_API_SECRET="<your_testnet_api_secret>"
python apps/run_realtime.py
```

Run live Binance mode on mainnet (explicit guard required):
```bash
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate qe
source scripts/qe_env.sh mainnet
export BINANCE_MAINNET_API_KEY="<your_mainnet_api_key>"
export BINANCE_MAINNET_API_SECRET="<your_mainnet_api_secret>"
export BINANCE_MAINNET_CONFIRM=YES
python apps/run_realtime.py
```

If you intentionally want to start mainnet with existing inventory, add:
```bash
export LIVE_ALLOW_NONFLAT_START=YES
```

### What you will see
- Trace JSONL at `artifacts/runs/_current/logs/trace.jsonl` (or `artifacts/runs/<run_id>/logs/trace.jsonl`).
- Soft-readiness warnings appear as `soft_domain.not_ready` entries in `artifacts/runs/_current/logs/default.jsonl`; this run does not emit a full PnL report.

## System philosophy (brief)
SoionLab separates responsibilities so each layer can enforce its own invariants: strategies declare structure, data handlers guard snapshot legality, and the driver owns time. The engine composes components but does not infer timestamps or data provenance.

| Component | Responsibility | Owns time? | Invariant |
| --- | --- | --- | --- |
| Strategy | Declare structure, symbols, and wiring | No | No timestamps or I/O in the strategy definition. |
| DataHandler | Cache ticks and align snapshots | No | Snapshots only expose data with `data_ts <= step_ts`. |
| Driver | Advance time, ingest ticks, call `engine.step()` | Yes | Step timestamps are monotonic and driver-issued. |

## Deep dive docs
- [`docs/runtime_semantics.md`](docs/runtime_semantics.md): driver-owned time, lifecycle ordering, and runtime flow.
- [`docs/ingestion_boundary.md`](docs/ingestion_boundary.md): ingestion boundary, readiness contracts, and async replay.
- [`docs/contract_spec.md`](docs/contract_spec.md): protocol interfaces and cross-layer boundaries.

## Installation details
For conda/apt-get setup and fuller environment notes, see [`docs/INSTALL.md`](docs/INSTALL.md).
