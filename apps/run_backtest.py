from __future__ import annotations

import asyncio
import argparse
from pathlib import Path

from quant_engine.utils.paths import data_root_from_file
from apps.run_code.backtest_app import run_backtest_app

# STRATEGY_NAME = "EXAMPLE"
# BIND_SYMBOLS = {"A": "BTCUSDT", "B": "ETHUSDT"}

# STRATEGY_NAME = "RSI-ADX-SIDEWAYS-FRACTIONAL" # "RSI-ADX-SIDEWAYS-FRACTIONAL" to turn on the fractional trading 
# BIND_SYMBOLS = {"A": "BTCUSDT", "window_RSI" : '14', "window_ADX": '14', "window_RSI_rolling": '5'}

STRATEGY_NAME = "RSI-IV-DYNAMICAL-FRACTIONAL"
BIND_SYMBOLS = {"A": "BTCUSDT", "alpha": "20.0", "beta": "0.1", "rsi_window": "14", "rsi_mean_window": "14", "iv_x_target": "0.0", "iv_tau_days": "30", "iv_freshness_ms": "600000"}

# START_TS = 1766966400000 - 600 * 24 * 60 * 60 * 1000  # 2025-11-29 00:00:00 UTC (epoch ms)
# END_TS = 1767052800000 + 3 * 60 * 60 * 1000     # 2025-12-30 00:00:00 UTC (epoch ms) + 3 hours buffer

START_TS = 1769506962229 + 60 * 60 * 1000 # Jan 27 2026 09:42:42.229
END_TS = 1769990099115 - 3 * 60 * 60 * 1000 # Feb 01 2026 23:54:59.115

DATA_ROOT = data_root_from_file(__file__, levels_up=1) ## default to src/quant_engine/data/


def _parse_bind_symbols(text: str) -> dict[str, str]:
    pairs = [part.strip() for part in str(text).split(",") if part.strip()]
    if not pairs:
        raise ValueError("bind symbols must not be empty")
    out: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"invalid bind symbol pair: {pair!r}; expected KEY=VALUE")
        k, v = pair.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k or not v:
            raise ValueError(f"invalid bind symbol pair: {pair!r}; expected KEY=VALUE")
        out[k] = v
    return out


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run backtest app")
    parser.add_argument("--strategy", default=STRATEGY_NAME, help="strategy name in registry")
    parser.add_argument(
        "--strategy-config",
        default=None,
        help="alias for --strategy (kept for ship-workflow compatibility)",
    )
    parser.add_argument(
        "--symbols",
        default=",".join(f"{k}={v}" for k, v in BIND_SYMBOLS.items()),
        help="symbol bindings, e.g. A=BTCUSDT,B=ETHUSDT",
    )
    parser.add_argument("--start-ts", type=int, default=int(START_TS), help="backtest start timestamp (epoch ms)")
    parser.add_argument("--end-ts", type=int, default=int(END_TS), help="backtest end timestamp (epoch ms)")
    parser.add_argument("--data-root", default=str(DATA_ROOT), help="data root path")
    parser.add_argument("--run-id", default=None, help="optional run_id override")
    return parser


async def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    strategy = str(args.strategy_config or args.strategy)
    bind_symbols = _parse_bind_symbols(str(args.symbols))
    data_root = Path(str(args.data_root))
    if int(args.start_ts) >= int(args.end_ts):
        raise ValueError(f"--start-ts must be < --end-ts, got {args.start_ts} >= {args.end_ts}")
    await run_backtest_app(
        strategy_name=strategy,
        bind_symbols=bind_symbols,
        start_ts=int(args.start_ts),
        end_ts=int(args.end_ts),
        data_root=data_root,
        run_id=str(args.run_id) if args.run_id else None,
    )


if __name__ == "__main__":
    asyncio.run(main())
