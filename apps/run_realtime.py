from __future__ import annotations

import argparse
import asyncio
import signal

# run_id format contract is implemented in run_code/realtime_app.py via: strftime("%Y%m%dT%H%M%SZ")

from quant_engine.utils.logger import init_logging

from apps.run_code.realtime_app import (
    BinanceClientError,
    DEFAULT_BIND_SYMBOLS,
    RealtimeDriver,
    StrategyLoader,
    _build_realtime_ingestion_plan,
    _install_signal_handlers,
    _make_run_id,
    _matching_type_for_strategy,
    _resolve_deribit_base_url,
    _set_current_run,
    _validate_realtime_preflight,
    build_realtime_engine,
    get_strategy,
    resolve_binance_profile,
    run_realtime_app as _run_realtime_app_impl,
)


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
    parser = argparse.ArgumentParser(description="Run realtime app")
    parser.add_argument("--strategy", default="EXAMPLE", help="strategy name in registry")
    parser.add_argument(
        "--strategy-config",
        default=None,
        help="alias for --strategy (kept for ship-workflow compatibility)",
    )
    parser.add_argument(
        "--symbols",
        default=",".join(f"{k}={v}" for k, v in DEFAULT_BIND_SYMBOLS.items()),
        help="symbol bindings, e.g. A=BTCUSDT,B=ETHUSDT",
    )
    parser.add_argument("--run-id", default=None, help="optional run_id override")
    parser.add_argument(
        "--binance-env",
        choices=["testnet", "mainnet"],
        default=None,
        help="override BINANCE_ENV for live-binance preflight and runtime",
    )
    parser.add_argument(
        "--binance-base-url",
        default=None,
        help="optional BINANCE_BASE_URL override; checked for profile mismatch risk",
    )
    parser.add_argument(
        "--deribit-base-url",
        default=None,
        help="optional Deribit base URL override for option-chain polling",
    )
    return parser


async def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    await _run_realtime_app_impl(
        strategy_name=str(args.strategy_config or args.strategy),
        bind_symbols=_parse_bind_symbols(str(args.symbols)),
        run_id=str(args.run_id) if args.run_id else None,
        binance_env=str(args.binance_env) if args.binance_env is not None else None,
        binance_base_url=str(args.binance_base_url) if args.binance_base_url is not None else None,
        deribit_base_url=str(args.deribit_base_url) if args.deribit_base_url is not None else None,
        validate_realtime_preflight_fn=_validate_realtime_preflight,
        build_realtime_engine_fn=build_realtime_engine,
        install_signal_handlers_fn=_install_signal_handlers,
        realtime_driver_cls=RealtimeDriver,
        init_logging_fn=init_logging,
        set_current_run_fn=_set_current_run,
    )


if __name__ == "__main__":
    asyncio.run(main())
