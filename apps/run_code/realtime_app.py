from __future__ import annotations

import argparse
import asyncio
import os
import signal
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from ingestion.ohlcv.worker import OHLCVWorker
from ingestion.ohlcv.source import BinanceKlinesRESTSource, OHLCVRESTSource
from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer
from ingestion.orderbook.worker import OrderbookWorker
from ingestion.orderbook.source import OrderbookWebSocketSource
from ingestion.orderbook.normalize import BinanceOrderbookNormalizer
from ingestion.contracts.tick import _to_interval_ms
from quant_engine.runtime.modes import EngineMode
from quant_engine.runtime.realtime import RealtimeDriver, DEFAULT_STEP_DELAY_MS
from quant_engine.health.restart import SourceRestartManager
from quant_engine.strategy.engine import StrategyEngine
from quant_engine.strategy.config import NormalizedStrategyCfg
from quant_engine.strategy.loader import StrategyLoader
from quant_engine.strategy.registry import get_strategy
from quant_engine.execution.exchange.binance_client import (
    BinanceClientError,
    BinanceSpotClient,
    resolve_binance_profile,
)
from quant_engine.contracts.exchange_account import StartupReadiness
from quant_engine.execution.exchange.account_adapter import BinanceAccountAdapter
from quant_engine.utils.asyncio import create_task_named
from quant_engine.utils.cleaned_path_resolver import base_asset_from_symbol
from quant_engine.utils.logger import (
    get_logger,
    init_logging,
    log_exception,
    log_info,
    log_warn,
    build_execution_constraints,
    build_trace_header,
    log_trace_header,
)


def _make_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _set_current_run(run_id: str) -> None:
    runs_dir = Path("artifacts") / "runs"
    current = runs_dir / "_current"
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / run_id).mkdir(parents=True, exist_ok=True)
    try:
        if current.exists() or current.is_symlink():
            current.unlink()
        current.symlink_to(run_id, target_is_directory=True)
    except (OSError, NotImplementedError):
        (runs_dir / "CURRENT").write_text(run_id, encoding="utf-8")


# Optional domains (enable when you have realtime sources for them).
# NOTE: If your strategy requires these domains (REQUIRED_DATA), you MUST wire them below
# or fail-fast (we do fail-fast).


logger = get_logger(__name__)
DEFAULT_BIND_SYMBOLS = {"A": "BTCUSDT", "B": "ETHUSDT"}
SHUTDOWN_TIMEOUT_S = 15.0
DEFAULT_REALTIME_OHLCV_POLL_INTERVAL_MS = 3_000
DEFAULT_REALTIME_OPTION_CHAIN_POLL_INTERVAL_MS = 5_000


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


def _resolve_realtime_step_delay_ms() -> int:
    raw = os.environ.get("REALTIME_STEP_DELAY_MS")
    if raw is None or not str(raw).strip():
        return int(DEFAULT_STEP_DELAY_MS)
    try:
        return max(0, int(str(raw).strip()))
    except Exception as exc:
        raise RuntimeError(
            f"REALTIME_STEP_DELAY_MS must be a non-negative integer, got: {raw!r}"
        ) from exc


def _resolve_realtime_ohlcv_poll_interval_ms() -> int:
    raw = os.environ.get("REALTIME_OHLCV_POLL_INTERVAL_MS")
    if raw is None or not str(raw).strip():
        return int(DEFAULT_REALTIME_OHLCV_POLL_INTERVAL_MS)
    try:
        return max(1, int(str(raw).strip()))
    except Exception as exc:
        raise RuntimeError(
            f"REALTIME_OHLCV_POLL_INTERVAL_MS must be a positive integer, got: {raw!r}"
        ) from exc


def _resolve_realtime_option_chain_poll_interval_ms() -> int:
    raw = os.environ.get("REALTIME_OPTION_CHAIN_POLL_INTERVAL_MS")
    if raw is None or not str(raw).strip():
        return int(DEFAULT_REALTIME_OPTION_CHAIN_POLL_INTERVAL_MS)
    try:
        return max(1, int(str(raw).strip()))
    except Exception as exc:
        raise RuntimeError(
            f"REALTIME_OPTION_CHAIN_POLL_INTERVAL_MS must be a positive integer, got: {raw!r}"
        ) from exc


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


def _resolve_deribit_base_url(*, binance_env: str | None, explicit: str | None) -> str:
    if explicit is not None and str(explicit).strip():
        return str(explicit).strip()
    env_override = os.environ.get("DERIBIT_BASE_URL")
    if env_override is not None and str(env_override).strip():
        return str(env_override).strip()
    profile = str(binance_env or os.environ.get("BINANCE_ENV", "testnet")).strip().lower()
    if profile == "testnet":
        return "https://test.deribit.com"
    return "https://www.deribit.com"


def _matching_type_for_strategy(*, strategy_name: str, bind_symbols: dict[str, str], overrides: dict | None = None) -> str:
    strategy_cls = get_strategy(strategy_name)
    cfg = strategy_cls.standardize(overrides=overrides or {}, symbols=bind_symbols)
    execution_cfg = cfg.execution if isinstance(cfg.execution, dict) else {}
    matching_cfg = execution_cfg.get("matching") if isinstance(execution_cfg, dict) else {}
    if not isinstance(matching_cfg, dict):
        return ""
    return str(matching_cfg.get("type", "")).strip().upper()


_STARTUP_READINESS_EPSILON = 1e-9


def evaluate_startup_readiness(
    readiness: StartupReadiness,
    *,
    is_mainnet: bool,
    allow_nonflat_start: bool = False,
) -> str | None:
    """Return a failure reason string, or None if readiness passes."""
    if readiness.open_order_count > 0:
        return (
            f"open orders exist for {readiness.symbol}: "
            f"count={readiness.open_order_count}"
        )
    if readiness.quote_locked > _STARTUP_READINESS_EPSILON:
        return (
            f"locked quote balance for {readiness.quote_asset}: "
            f"locked={readiness.quote_locked}"
        )
    if is_mainnet and not allow_nonflat_start:
        if abs(readiness.base_position_qty) > _STARTUP_READINESS_EPSILON:
            return (
                f"non-flat startup inventory for {readiness.symbol}: "
                f"qty={readiness.base_position_qty}; "
                "set LIVE_ALLOW_NONFLAT_START=YES to override"
            )
    return None


def _validate_realtime_preflight(
    *,
    strategy_name: str,
    bind_symbols: dict[str, str],
    binance_env: str | None,
    binance_base_url: str | None,
    force_live_matching: bool = False,
) -> None:
    matching_type = "LIVE-BINANCE" if force_live_matching else _matching_type_for_strategy(
        strategy_name=strategy_name,
        bind_symbols=bind_symbols,
        overrides=None,
    )
    if matching_type != "LIVE-BINANCE":
        return
    explicit_base_url = binance_base_url if binance_base_url is not None else os.environ.get("BINANCE_BASE_URL")
    if explicit_base_url is not None:
        override_confirm = str(os.environ.get("BINANCE_BASE_URL_CONFIRM", "")).strip()
        proxy_mode = str(os.environ.get("BINANCE_PROXY_MODE", "")).strip()
        if override_confirm != "YES" and proxy_mode != "1":
            log_warn(
                logger,
                "binance.base_url.override.blocked",
                reason="missing_explicit_opt_in",
                required_any_of=["BINANCE_BASE_URL_CONFIRM=YES", "BINANCE_PROXY_MODE=1"],
            )
            raise RuntimeError(
                "Realtime preflight blocked: BINANCE_BASE_URL override is privileged and requires "
                "BINANCE_BASE_URL_CONFIRM=YES or BINANCE_PROXY_MODE=1."
            )
    try:
        cfg = resolve_binance_profile(
            env=binance_env,
            base_url=binance_base_url,
        )
    except BinanceClientError as exc:
        raise RuntimeError(
            "Realtime preflight failed for LIVE-BINANCE. "
            "Set BINANCE_ENV plus matching API key/secret env vars and verify BINANCE_BASE_URL profile consistency. "
            f"Details: {exc}"
        ) from exc
    if cfg.env == "mainnet" and str(os.environ.get("BINANCE_MAINNET_CONFIRM", "")).strip() != "YES":
        raise RuntimeError(
            "Realtime preflight blocked: BINANCE_ENV=mainnet requires BINANCE_MAINNET_CONFIRM=YES before startup."
        )
def _install_signal_handlers(loop: asyncio.AbstractEventLoop, on_signal) -> tuple[str, list[signal.Signals], dict[signal.Signals, Any]]:
    installed: list[signal.Signals] = []
    previous: dict[signal.Signals, Any] = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, on_signal, sig)
            installed.append(sig)
        except (NotImplementedError, RuntimeError, ValueError):
            installed = []
            break
    if installed:
        return "loop", installed, previous

    def _fallback_handler(sig_num: int, _frame: Any) -> None:
        try:
            sig = signal.Signals(sig_num)
        except Exception:
            return
        loop.call_soon_threadsafe(on_signal, sig)

    for sig in (signal.SIGINT, signal.SIGTERM):
        previous[sig] = signal.getsignal(sig)
        signal.signal(sig, _fallback_handler)
        installed.append(sig)
    return "signal", installed, previous


def _build_realtime_ingestion_plan(
    engine: StrategyEngine,
    *,
    required_domains: set[str],
    stop_event: threading.Event | None = None,
    deribit_base_url: str | None = None,
) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    ohlcv_poll_interval_ms = _resolve_realtime_ohlcv_poll_interval_ms()
    option_chain_poll_interval_ms = _resolve_realtime_option_chain_poll_interval_ms()

    def make_emit(primary_handler, *extra_handlers):
        def emit(tick):
            # Invariant: realtime ticks must cross engine ingest boundary — enforced here to prevent health intercept bypass
            engine.ingest_tick(tick)
            for h in extra_handlers:
                try:
                    h.on_new_tick(tick)
                except Exception:
                    log_exception(logger, "emit fan-out failed", handler=h, tick=tick)
        return emit

    def attach_backfill_worker(handler: Any, worker: Any, emit: Any) -> None:
        setter = getattr(handler, "set_external_source", None)
        if callable(setter):
            setter(worker, emit=emit)

    # ---------- OHLCV (websocket) ----------
    for symbol, handler in engine.ohlcv_handlers.items():
        emit = make_emit(handler)

        def _build_worker_ohlcv(symbol: str = symbol, handler: Any = handler, emit=emit):
            # realtime OHLCV must use REST source here so raw persistence/backfill semantics are available
            source = cast(
                OHLCVRESTSource,
                BinanceKlinesRESTSource(
                    symbol=symbol,
                    interval=str(getattr(handler, "interval", "1m")),
                    poll_interval_ms=int(ohlcv_poll_interval_ms),
                    stop_event=stop_event,
                ),
            )
            normalizer = BinanceOHLCVNormalizer(symbol=symbol)
            interval = getattr(handler, "interval", None)
            interval_ms = _to_interval_ms(interval) if isinstance(interval, str) and interval else None
            worker = OHLCVWorker(
                source=source,
                fetch_source=source,
                normalizer=normalizer,
                symbol=symbol,
                interval=str(interval) if interval else None,
                interval_ms=int(interval_ms) if interval_ms is not None else None,
                poll_interval_ms=int(ohlcv_poll_interval_ms),
            )
            attach_backfill_worker(handler, worker, emit)
            return worker

        plan.append(
            {
                "domain": "ohlcv",
                "symbol": symbol,
                "build_worker": _build_worker_ohlcv,
                "emit": emit,
            }
        )

    # ---------- Orderbook (websocket) ----------
    for symbol, handler in engine.orderbook_handlers.items():
        emit = make_emit(handler)

        def _build_worker_orderbook(symbol: str = symbol, handler: Any = handler, emit=emit):
            # source = OrderbookWebSocketSource(symbol=symbol, depth=handler.depth)
            source = OrderbookWebSocketSource()
            normalizer = BinanceOrderbookNormalizer(symbol=symbol)
            interval = getattr(handler, "interval", None)
            interval_ms = _to_interval_ms(interval) if isinstance(interval, str) and interval else None
            worker = OrderbookWorker(
                source=source,
                normalizer=normalizer,
                symbol=symbol,
                interval=str(interval) if interval else None,
                interval_ms=int(interval_ms) if interval_ms is not None else None,
            )
            attach_backfill_worker(handler, worker, emit)
            return worker

        plan.append(
            {
                "domain": "orderbook",
                "symbol": symbol,
                "build_worker": _build_worker_orderbook,
                "emit": emit,
            }
        )

    # ---------- Option chain (poll/websocket) ----------
    if engine.option_chain_handlers:
        try:
            from ingestion.option_chain.worker import OptionChainWorker
            from ingestion.option_chain.source import DeribitOptionChainRESTSource
            from ingestion.option_chain.normalize import DeribitOptionChainNormalizer
        except Exception as e:
            if "option_chain" in required_domains:
                raise RuntimeError(
                    "Strategy requires option_chain, but realtime ingestion wiring is missing. "
                    "Implement/enable ingestion.option_chain.{source,worker,normalize} realtime sources."
                ) from e
            OptionChainWorker = None  # type: ignore
        if OptionChainWorker is not None:
            for asset, ch in engine.option_chain_handlers.items():
                # Prefer polling unless you explicitly implement websocket
                ivh = engine.iv_surface_handlers.get(asset)
                emit = make_emit(ch, ivh) if ivh is not None else make_emit(ch)

                def _build_worker_option_chain(asset: str = asset, handler: Any = ch, emit=emit):
                    # option_chain live path uses REST polling so snapshots are persisted to raw and backfillable
                    # Deribit expects base currency (e.g. "BTC"), not Binance-style pair (e.g. "BTCUSDT")
                    source = DeribitOptionChainRESTSource(
                        currency=base_asset_from_symbol(asset),
                        interval=str(getattr(handler, "interval", "1m")),
                        poll_interval_ms=int(option_chain_poll_interval_ms),
                        base_url=str(deribit_base_url or "https://www.deribit.com"),
                        stop_event=stop_event,
                    )
                    normalizer = DeribitOptionChainNormalizer(symbol=asset)
                    interval = getattr(handler, "interval", None)
                    interval_ms = _to_interval_ms(interval) if isinstance(interval, str) and interval else None
                    worker = OptionChainWorker(
                        source=source,
                        fetch_source=source,
                        normalizer=normalizer,
                        symbol=asset,
                        interval=str(interval) if interval else None,
                        interval_ms=int(interval_ms) if interval_ms is not None else None,
                        poll_interval_ms=int(option_chain_poll_interval_ms),
                    )
                    attach_backfill_worker(handler, worker, emit)
                    return worker

                # If IV surface handler exists for this asset, feed it the same chain ticks

                plan.append(
                    {
                        "domain": "option_chain",
                        "symbol": asset,
                        "build_worker": _build_worker_option_chain,
                        "emit": emit,
                    }
                )

    # ---------- Sentiment (poll) ----------
    if engine.sentiment_handlers:
        try:
            from ingestion.sentiment.worker import SentimentWorker
            from ingestion.sentiment.source import SentimentStreamSource
            from ingestion.sentiment.normalize import SentimentNormalizer
        except Exception as e:
            if "sentiment" in required_domains:
                raise RuntimeError(
                    "Strategy requires sentiment, but realtime ingestion wiring is missing. "
                    "Implement/enable ingestion.sentiment.{source,worker,normalize} realtime sources."
                ) from e
            SentimentWorker = None  # type: ignore
        if SentimentWorker is not None:
            for src, sh in engine.sentiment_handlers.items():
                emit = make_emit(sh)

                # source = SentimentRESTSource(source=src, interval=sh.interval)
                def _build_worker_sentiment(src: str = src, handler: Any = sh, emit=emit):
                    source = SentimentStreamSource()
                    normalizer = SentimentNormalizer(symbol=src, provider=src)
                    interval = getattr(handler, "interval", None)
                    interval_ms = _to_interval_ms(interval) if isinstance(interval, str) and interval else None
                    worker = SentimentWorker(
                        source=source,
                        normalizer=normalizer,
                        interval=str(interval) if interval else None,
                        interval_ms=int(interval_ms) if interval_ms is not None else None,
                    )
                    attach_backfill_worker(handler, worker, emit)
                    return worker

                plan.append(
                    {
                        "domain": "sentiment",
                        "symbol": src,
                        "build_worker": _build_worker_sentiment,
                        "emit": emit,
                    }
                )

    return plan


def build_realtime_engine(
    *,
    strategy_name: str = "EXAMPLE",
    bind_symbols: dict[str, str] | None = None,
    overrides: dict | None = None,
    force_live_matching: bool = False,
    stop_event: threading.Event | None = None,
    deribit_base_url: str | None = None,
) -> tuple[StrategyEngine, dict[str, Any], list[dict[str, Any]]]:
    StrategyCls = get_strategy(strategy_name)
    if bind_symbols is None:
        bind_symbols = dict(DEFAULT_BIND_SYMBOLS)
    cfg_obj = StrategyCls.standardize(overrides=overrides or {}, symbols=bind_symbols)
    cfg = cfg_obj.to_dict() if isinstance(cfg_obj, NormalizedStrategyCfg) else dict(cfg_obj)

    execution_cfg = cfg.get("execution") if isinstance(cfg, dict) else {}
    if not isinstance(execution_cfg, dict):
        execution_cfg = {}
    matching_cfg = execution_cfg.get("matching")
    if not isinstance(matching_cfg, dict):
        matching_cfg = {}
        execution_cfg["matching"] = matching_cfg
    if force_live_matching:
        matching_cfg["type"] = "LIVE-BINANCE"
        execution_cfg["matching"] = matching_cfg
        cfg["execution"] = execution_cfg

    matching_type = str(matching_cfg.get("type", "")).strip().upper()
    exchange_account_adapter = None
    exchange_symbol_constraints = None
    symbol = ""
    if matching_type == "LIVE-BINANCE":
        symbol = str(cfg.get("symbol") or "").strip().upper()
        if not symbol:
            raise RuntimeError("Realtime preflight failed: missing primary symbol for LIVE-BINANCE")
        profile = resolve_binance_profile()
        client = BinanceSpotClient(
            api_key=profile.api_key,
            api_secret=profile.api_secret,
            base_url=profile.base_url,
            recv_window=int(profile.recv_window),
            time_sync_interval_s=float(profile.time_sync_interval_s),
            timeout_s=float(profile.timeout_s),
        )
        exchange_account_adapter = BinanceAccountAdapter(client)
        try:
            exchange_symbol_constraints = exchange_account_adapter.get_symbol_constraints(symbol)
        except BinanceClientError as exc:
            raise RuntimeError(
                "Realtime preflight failed for LIVE-BINANCE metadata fetch. "
                f"Details: {exc}"
            ) from exc
        matching_params = matching_cfg.get("params")
        if not isinstance(matching_params, dict):
            matching_params = {}
        matching_params["client"] = client
        matching_cfg["params"] = matching_params
        execution_cfg["matching"] = matching_cfg
        cfg["execution"] = execution_cfg

    engine = StrategyLoader.from_config(
        strategy=cfg,
        mode=EngineMode.REALTIME,
        overrides={},
    )
    if matching_type == "LIVE-BINANCE":
        if exchange_account_adapter is None or exchange_symbol_constraints is None:
            raise RuntimeError("Realtime startup failed: exchange account adapter missing for LIVE-BINANCE")

        # --- Startup readiness gate (before any portfolio mutation) ---
        try:
            readiness = exchange_account_adapter.get_startup_readiness(symbol)
        except BinanceClientError as exc:
            raise RuntimeError(
                "Realtime startup readiness query failed for LIVE-BINANCE. "
                f"Details: {exc}"
            ) from exc

        is_mainnet = str(os.environ.get("BINANCE_ENV", "")).strip().lower() == "mainnet"
        allow_nonflat = str(os.environ.get("LIVE_ALLOW_NONFLAT_START", "")).strip() == "YES"
        failure = evaluate_startup_readiness(
            readiness, is_mainnet=is_mainnet, allow_nonflat_start=allow_nonflat,
        )
        if failure is not None:
            log_warn(
                logger,
                "realtime.startup.readiness_blocked",
                symbol=readiness.symbol,
                open_order_count=readiness.open_order_count,
                quote_locked=readiness.quote_locked,
                base_position_qty=readiness.base_position_qty,
                mainnet=is_mainnet,
                allow_nonflat_start=allow_nonflat,
                reason=failure,
            )
            raise RuntimeError(f"Realtime startup blocked: {failure}")
        log_info(
            logger,
            "realtime.startup.readiness_passed",
            symbol=readiness.symbol,
            open_order_count=readiness.open_order_count,
            quote_locked=readiness.quote_locked,
            base_position_qty=readiness.base_position_qty,
            mainnet=is_mainnet,
            allow_nonflat_start=allow_nonflat,
        )

        # --- Exchange account sync (only after readiness passes) ---
        try:
            account_state = exchange_account_adapter.get_account_state()
        except BinanceClientError as exc:
            raise RuntimeError(
                "Realtime startup sync failed for LIVE-BINANCE account fetch. "
                f"Details: {exc}"
            ) from exc
        if exchange_symbol_constraints.quote_asset is None:
            raise RuntimeError(f"Realtime startup sync failed: missing quote asset for {symbol}")
        quote_balance = account_state.balances.get(exchange_symbol_constraints.quote_asset)
        if quote_balance is None:
            raise RuntimeError(
                f"Realtime startup sync failed: quote asset {exchange_symbol_constraints.quote_asset} "
                f"not present in exchange balances for {symbol}"
            )
        engine.portfolio.sync_from_exchange(
            account_state,
            symbol=symbol,
            quote_asset=exchange_symbol_constraints.quote_asset,
        )
        setattr(engine, "exchange_account_adapter", exchange_account_adapter)
        setattr(engine, "exchange_account_symbol", symbol)
        setattr(engine, "exchange_symbol_constraints", exchange_symbol_constraints)
        base_position_qty = float(account_state.positions.get(symbol, 0.0) or 0.0)
        if abs(base_position_qty) > 1e-12:
            log_warn(
                logger,
                "realtime.portfolio.startup_position_synced",
                symbol=symbol,
                exchange_position_qty=base_position_qty,
                reason="portfolio position quantity synced from exchange; existing live inventory was present at startup",
            )
        log_info(
            logger,
            "realtime.portfolio.startup_cash_synced",
            symbol=symbol,
            quote_asset=exchange_symbol_constraints.quote_asset,
            exchange_quote_free=float(quote_balance.free),
            exchange_quote_locked=float(quote_balance.locked),
        )

    required_data = getattr(StrategyCls, "REQUIRED_DATA", None)
    if isinstance(required_data, set):
        required_domains = {str(x) for x in required_data}
    elif isinstance(required_data, (list, tuple)):
        required_domains = {str(x) for x in required_data}
    else:
        required_domains = set()

    if required_domains:
        missing: list[str] = []
        if "ohlcv" in required_domains and not engine.ohlcv_handlers:
            missing.append("ohlcv")
        if "orderbook" in required_domains and not engine.orderbook_handlers:
            missing.append("orderbook")
        if "option_chain" in required_domains and not engine.option_chain_handlers:
            missing.append("option_chain")
        if "sentiment" in required_domains and not engine.sentiment_handlers:
            missing.append("sentiment")
        if "iv_surface" in required_domains and not engine.iv_surface_handlers:
            missing.append("iv_surface")
        if missing:
            raise RuntimeError(f"Strategy requires domains not present in engine: {missing}")
        non_persistent_required: list[str] = []
        # required live domains must use raw-persistent ingestion sources; optional domains can remain stream-only
        if "orderbook" in required_domains and engine.orderbook_handlers:
            non_persistent_required.append("orderbook")
        if "sentiment" in required_domains and engine.sentiment_handlers:
            non_persistent_required.append("sentiment")
        if "trades" in required_domains and engine.trades_handlers:
            non_persistent_required.append("trades")
        if "option_trades" in required_domains and engine.option_trades_handlers:
            non_persistent_required.append("option_trades")
        if non_persistent_required:
            raise RuntimeError(
                "Strategy requires domains without raw-persistent realtime source wiring: "
                f"{sorted(non_persistent_required)}"
            )

    ingestion_plan = _build_realtime_ingestion_plan(
        engine,
        required_domains=required_domains,
        stop_event=stop_event,
        deribit_base_url=deribit_base_url,
    )
    driver_cfg: dict[str, Any] = {}
    return engine, driver_cfg, ingestion_plan


async def run_realtime_app(
    *,
    strategy_name: str = "EXAMPLE",
    bind_symbols: dict[str, str] | None = None,
    run_id: str | None = None,
    binance_env: str | None = None,
    binance_base_url: str | None = None,
    deribit_base_url: str | None = None,
    validate_realtime_preflight_fn=_validate_realtime_preflight,
    build_realtime_engine_fn=build_realtime_engine,
    install_signal_handlers_fn=_install_signal_handlers,
    realtime_driver_cls=RealtimeDriver,
    init_logging_fn=init_logging,
    set_current_run_fn=_set_current_run,
) -> None:
    bind_symbols = dict(bind_symbols or DEFAULT_BIND_SYMBOLS)
    old_binance_env = os.environ.get("BINANCE_ENV")
    old_binance_base_url = os.environ.get("BINANCE_BASE_URL")
    if binance_env is not None:
        os.environ["BINANCE_ENV"] = str(binance_env)
    if binance_base_url is not None:
        os.environ["BINANCE_BASE_URL"] = str(binance_base_url)
    try:
        validate_realtime_preflight_fn(
            strategy_name=strategy_name,
            bind_symbols=bind_symbols,
            binance_env=str(binance_env) if binance_env is not None else None,
            binance_base_url=str(binance_base_url) if binance_base_url is not None else None,
            force_live_matching=True,
        )
        effective_deribit_base_url = _resolve_deribit_base_url(
            binance_env=str(binance_env) if binance_env is not None else None,
            explicit=str(deribit_base_url) if deribit_base_url is not None else None,
        )

        stop_event = threading.Event()
        run_id = str(run_id or _make_run_id())
        init_logging_fn(run_id=run_id)
        set_current_run_fn(run_id)

        engine, _driver_cfg, ingestion_plan = build_realtime_engine_fn(
            strategy_name=strategy_name,
            bind_symbols=bind_symbols,
            force_live_matching=True,
            stop_event=stop_event,
            deribit_base_url=effective_deribit_base_url,
        )
        log_trace_header(
            logger,
            build_trace_header(
                run_id=run_id,
                engine_mode=engine.spec.mode.value,
                config_hash=getattr(engine, "config_hash", "unknown"),
                strategy_name=getattr(engine, "strategy_name", "unknown"),
                interval=engine.spec.interval,
                execution_constraints=build_execution_constraints(engine.portfolio),
            ),
        )

        health = getattr(engine, "_health", None)
        restart_manager = SourceRestartManager(health=health, logger=logger) if health is not None else None
        ingestion_tasks: list[asyncio.Task[None]] = []
        driver_task: asyncio.Task[None] | None = None
        shutdown_signal_count = 0

        def _start_worker(entry: dict[str, Any]) -> asyncio.Task[None]:
            domain = str(entry["domain"])
            symbol = str(entry["symbol"])
            worker = entry["build_worker"]()

            def _on_restart() -> None:
                if restart_manager is None:
                    return
                restart_manager.schedule_restart(
                    domain=domain,
                    symbol=symbol,
                    factory=lambda: _start_worker(entry),
                    stop_event=stop_event,
                )

            task = create_task_named(
                worker.run(emit=entry["emit"]),
                name=f"ingestion.{domain}:{symbol}",
                logger=logger,
                context={
                    "domain": domain,
                    "symbol": symbol,
                },
                stop_event=stop_event,
                health=health,
                health_domain=domain,
                health_symbol=symbol,
                on_restart=_on_restart if restart_manager is not None else None,
            )
            ingestion_tasks.append(task)
            return task

        for entry in ingestion_plan:
            _start_worker(entry)

        logger.info("Realtime ingestion workers started.")

        # -------------------------------------------------
        # 4) Run realtime driver (single time authority)
        # -------------------------------------------------
        step_delay_ms = _resolve_realtime_step_delay_ms()
        log_info(
            logger,
            "realtime.driver.step_delay_configured",
            step_delay_ms=int(step_delay_ms),
        )
        driver = realtime_driver_cls(
            engine=engine,
            spec=engine.spec,
            stop_event=stop_event,
            step_delay_ms=int(step_delay_ms),
        )
        loop = asyncio.get_running_loop()

        def _request_shutdown(sig: signal.Signals) -> None:
            nonlocal shutdown_signal_count
            shutdown_signal_count += 1
            if stop_event.is_set():
                return
            stop_event.set()
            logger.warning("app.shutdown.signal", extra={"context": {"signal": sig.name}})
            if restart_manager is not None:
                restart_manager.cancel_all()
            for t in ingestion_tasks:
                t.cancel()
            if driver_task is not None and not driver_task.done():
                driver_task.cancel()

        signal_mode, installed_signals, previous_handlers = install_signal_handlers_fn(loop, _request_shutdown)

        try:
            logger.info("Starting realtime driver...")
            driver_task = asyncio.create_task(driver.run(), name="runtime.realtime_driver")
            await driver_task
        except asyncio.CancelledError:
            stop_event.set()
        finally:
            if signal_mode == "loop":
                for sig in installed_signals:
                    loop.remove_signal_handler(sig)
            elif signal_mode == "signal":
                for sig in installed_signals:
                    prev = previous_handlers.get(sig, signal.SIG_DFL)
                    signal.signal(sig, prev)
            logger.info("Shutting down ingestion workers...")
            # Invariant: restart-manager pending tasks must be cancelled before ingestion teardown — enforced here to prevent shutdown-lingering restart delays
            if restart_manager is not None:
                restart_manager.cancel_all()
            stop_event.set()
            for t in ingestion_tasks:
                t.cancel()
            if driver_task is not None and not driver_task.done():
                driver_task.cancel()
            wait_tasks: list[asyncio.Future[Any] | asyncio.Task[Any]] = []
            if ingestion_tasks:
                wait_tasks.append(asyncio.gather(*ingestion_tasks, return_exceptions=True))
            if driver_task is not None:
                wait_tasks.append(asyncio.gather(driver_task, return_exceptions=True))
            if wait_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*wait_tasks, return_exceptions=True),
                        timeout=SHUTDOWN_TIMEOUT_S,
                    )
                except asyncio.TimeoutError:
                    log_warn(
                        logger,
                        "app.shutdown.timeout",
                        timeout_s=SHUTDOWN_TIMEOUT_S,
                        ingestion_tasks=len(ingestion_tasks),
                    )
    finally:
        if old_binance_env is None:
            os.environ.pop("BINANCE_ENV", None)
        else:
            os.environ["BINANCE_ENV"] = old_binance_env
        if old_binance_base_url is None:
            os.environ.pop("BINANCE_BASE_URL", None)
        else:
            os.environ["BINANCE_BASE_URL"] = old_binance_base_url


async def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    await run_realtime_app(
        strategy_name=str(args.strategy_config or args.strategy),
        bind_symbols=_parse_bind_symbols(str(args.symbols)),
        run_id=str(args.run_id) if args.run_id else None,
        binance_env=str(args.binance_env) if args.binance_env is not None else None,
        binance_base_url=str(args.binance_base_url) if args.binance_base_url is not None else None,
        deribit_base_url=str(args.deribit_base_url) if args.deribit_base_url is not None else None,
    )


if __name__ == "__main__":
    asyncio.run(main())
