# Sample Data Disclaimer

The files under `data/sample/` are **small, fixed demonstration datasets**
bundled solely to make the Quick Start runnable on a fresh clone.

## Purpose

These datasets exist only to:
- validate runtime wiring and engine initialization
- demonstrate driver-owned time and readiness gates
- produce trace/log artifacts for inspection

They are **not** intended for:
- performance evaluation
- profitability analysis
- market modeling or statistical inference

## Data characteristics

- Sources: Binance (aggregated OHLCV), Deribit (low-frequency option chain data)
- Scope: limited symbols and short time windows
- Frequency: reduced / aggregated (not tick-level)
- Stability: treated as frozen fixtures and may lag current exchange schemas

The data has been **minimally processed** for demonstration purposes and
may omit fields required for real research or trading use.

## No guarantees

The sample data is provided on a best-effort basis:
- no guarantee of completeness, accuracy, or continuity
- no guarantee of alignment with current exchange specifications
- no guarantee of suitability for any particular use case

## Attribution and removal

Data is derived from publicly accessible exchange outputs.
If you believe redistribution of any bundled file is inappropriate,
please open an issue or contact the author, and it will be reviewed promptly.