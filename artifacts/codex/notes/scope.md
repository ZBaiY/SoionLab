# Scope

Goal: enforce worker-owned backfill persistence to `raw/`, and isolate runtime from direct Source calls. Keep runtime semantics unchanged and preserve determinism.

In-scope modules and functions (minimal touch):
- Ingestion file sources (raw persistence):
  - `src/ingestion/ohlcv/source.py`, `src/ingestion/orderbook/source.py`, `src/ingestion/trades/source.py`
  - `src/ingestion/option_chain/source.py`, `src/ingestion/option_trades/source.py`, `src/ingestion/sentiment/source.py`
- Ingestion workers (backfill path):
  - `src/ingestion/ohlcv/worker.py`, `src/ingestion/orderbook/worker.py`, `src/ingestion/trades/worker.py`
  - `src/ingestion/option_chain/worker.py`, `src/ingestion/option_trades/worker.py`, `src/ingestion/sentiment/worker.py`
- Runtime handlers (delegate to workers, no Source access):
  - `src/quant_engine/data/ohlcv/realtime.py`, `src/quant_engine/data/orderbook/realtime.py`
  - `src/quant_engine/data/trades/realtime.py`, `src/quant_engine/data/derivatives/option_chain/chain_handler.py`
  - `src/quant_engine/data/derivatives/option_trades/realtime.py`, `src/quant_engine/data/sentiment/sentiment_handler.py`
  - `src/quant_engine/data/derivatives/iv/iv_handler.py` (explicit skip)
- Runtime/app wiring:
  - `apps/run_realtime.py`
- Resolver utilities:
  - `src/quant_engine/utils/cleaned_path_resolver.py` (raw path mirror)

Out of scope:
- Strategy definitions or engine/driver step ordering.
- Architecture refactors; only glue and persistence paths.

Hard constraints honored:
- Backfill uses worker-owned fetch+persist; runtime never calls Source methods.
- Raw persistence mirrors cleaned layout under `raw/`.
- Determinism preserved; backfill anchors to driver timestamps.
