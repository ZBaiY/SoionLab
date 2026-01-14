from __future__ import annotations

from typing import Any
from collections.abc import Iterable, Mapping
from quant_engine.contracts.decision import DecisionProto
from quant_engine.contracts.execution.engine import ExecutionEngineProto
from quant_engine.contracts.feature import FeatureExtractorProto
from quant_engine.contracts.model import ModelProto
from quant_engine.contracts.portfolio import PortfolioManagerProto, PortfolioState
from quant_engine.contracts.risk import RiskEngineProto
from quant_engine.data.contracts.snapshot import Snapshot
from quant_engine.runtime.snapshot import EngineSnapshot, SCHEMA_VERSION as RUNTIME_SNAPSHOT_SCHEMA
from quant_engine.runtime.context import SCHEMA_VERSION as RUNTIME_CONTEXT_SCHEMA
from quant_engine.contracts.decision import SCHEMA_VERSION as DECISION_SCHEMA
from quant_engine.contracts.risk import SCHEMA_VERSION as RISK_SCHEMA
from quant_engine.execution.engine import SCHEMA_VERSION as EXECUTION_SCHEMA
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.data.contracts.protocol_realtime import DataHandlerProto, OHLCVHandlerProto, RealTimeDataHandler
from quant_engine.utils.logger import get_logger, log_debug, log_warn, log_info, log_error, log_step_trace
from quant_engine.utils.num import visible_end_ts
from quant_engine.exceptions.core import FatalError
from quant_engine.utils.cleaned_path_resolver import normalize_symbol, symbol_matches
from quant_engine.utils.guards import (
    assert_monotonic,
    assert_no_lookahead,
    assert_schema_subset,
    ensure_epoch_ms,
)
from ingestion.contracts.tick import IngestionTick as Tick


def format_tick_key(domain: str | None, symbol: str | None, source_id: object | None) -> str:
    return f"{domain}:{symbol}:{source_id}"


class StrategyEngine:
    """Orchestrator of the entire quant pipeline.

    It does not compute features or model predictions itself;
    it only coordinates each Layer.
    """

    _logger = get_logger(__name__)
    LAYER_SCHEMA_VERSION = 2
    EXPECTED_MARKET_SCHEMA_VERSION = 2
    STEP_ORDER = (
        "handlers",
        "features",
        "models",
        "decision",
        "risk",
        "execution",
        "portfolio",
        "snapshot",
    )

    def __init__(
        self,
        *,
        spec: EngineSpec,
        ohlcv_handlers: Mapping[str, OHLCVHandlerProto],
        orderbook_handlers: Mapping[str, RealTimeDataHandler],
        option_chain_handlers: Mapping[str, RealTimeDataHandler],
        iv_surface_handlers: Mapping[str, RealTimeDataHandler],
        sentiment_handlers: Mapping[str, RealTimeDataHandler],
        trades_handlers: Mapping[str, RealTimeDataHandler],
        option_trades_handlers: Mapping[str, RealTimeDataHandler],
        feature_extractor: FeatureExtractorProto,
        models: Mapping[str, ModelProto],
        decision: DecisionProto,
        risk_manager: RiskEngineProto,
        execution_engine: ExecutionEngineProto,
        portfolio_manager: PortfolioManagerProto,
        guardrails: bool = True,
    ):
        self.spec = spec
        self.mode = spec.mode
        self.symbol = spec.symbol
        self.universe = spec.universe or {}
        self.ohlcv_handlers: Mapping[str, OHLCVHandlerProto] = ohlcv_handlers
        self.orderbook_handlers: Mapping[str, RealTimeDataHandler] = orderbook_handlers
        self.option_chain_handlers: Mapping[str, RealTimeDataHandler] = option_chain_handlers
        self.iv_surface_handlers: Mapping[str, RealTimeDataHandler] = iv_surface_handlers
        self.sentiment_handlers: Mapping[str, RealTimeDataHandler] = sentiment_handlers
        self.trades_handlers: Mapping[str, RealTimeDataHandler] = trades_handlers
        self.option_trades_handlers: Mapping[str, RealTimeDataHandler] = option_trades_handlers
        self.feature_extractor = feature_extractor
        self.models = models
        self.decision = decision
        self.risk_manager = risk_manager
        self.execution_engine = execution_engine
        self.portfolio = portfolio_manager
        self.engine_snapshot = EngineSnapshot(
            0,
            self.mode,
            {},
            {},
            None,
            None,
            [],
            portfolio_manager.state(),
        )
        self._guardrails_enabled = bool(guardrails)
        self._step_stage_index = 0
        log_debug(self._logger, "StrategyEngine initialized",
                  mode=self.spec.mode.value,
                  model_count=len(models))
        self._warn_schema_mismatches()
        self._validate_handler_contracts()
        self._validate_component_contracts()
        counts = {
            "ohlcv": len(self.ohlcv_handlers),
            "orderbook": len(self.orderbook_handlers),
            "option_chain": len(self.option_chain_handlers),
            "iv_surface": len(self.iv_surface_handlers),
            "sentiment": len(self.sentiment_handlers),
            "trades": len(self.trades_handlers),
            "option_trades": len(self.option_trades_handlers),
        }

        domains_present = [k for k, v in counts.items() if v]
        log_debug(
            self._logger,
            "engine.handlers.bound",
            mode=self.spec.mode.value,
            symbol=self.symbol,
            domains_present=domains_present,
            counts_per_domain=counts,
        )

        self._log_wiring()
        self._last_step_ts: int | None = None
        self._last_tick_ts_by_key: dict[str, int] = {}
        self._snapshot_schema_keys: dict[str, set[str]] = {}
        self._unhandled_domains_logged: set[str] = set()
        self._unhandled_symbols_logged: set[str] = set()
        self._unhandled_sources_logged: set[str] = set()
        self._empty_snapshot_logged: set[str] = set()
        self.strategy_name = "<unnamed>"
        self.config_hash = "<no-hash>"
    def _warn_schema_mismatches(self) -> None:
        if self.LAYER_SCHEMA_VERSION != self.EXPECTED_MARKET_SCHEMA_VERSION:
            log_warn(
                self._logger,
                "Strategy layer schema is behind market schema",
                layer_schema=self.LAYER_SCHEMA_VERSION,
                expected_schema=self.EXPECTED_MARKET_SCHEMA_VERSION,
            )

        if RUNTIME_SNAPSHOT_SCHEMA != self.EXPECTED_MARKET_SCHEMA_VERSION:
            log_warn(
                self._logger,
                "Runtime snapshot schema is behind market schema",
                runtime_snapshot_schema=RUNTIME_SNAPSHOT_SCHEMA,
                expected_schema=self.EXPECTED_MARKET_SCHEMA_VERSION,
            )

        if RUNTIME_CONTEXT_SCHEMA != self.EXPECTED_MARKET_SCHEMA_VERSION:
            log_warn(
                self._logger,
                "Runtime context schema is behind market schema",
                runtime_context_schema=RUNTIME_CONTEXT_SCHEMA,
                expected_schema=self.EXPECTED_MARKET_SCHEMA_VERSION,
            )

        if DECISION_SCHEMA != self.EXPECTED_MARKET_SCHEMA_VERSION:
            log_warn(
                self._logger,
                "Decision schema is behind market schema",
                decision_schema=DECISION_SCHEMA,
                expected_schema=self.EXPECTED_MARKET_SCHEMA_VERSION,
            )

        if RISK_SCHEMA != self.EXPECTED_MARKET_SCHEMA_VERSION:
            log_warn(
                self._logger,
                "Risk schema is behind market schema",
                risk_schema=RISK_SCHEMA,
                expected_schema=self.EXPECTED_MARKET_SCHEMA_VERSION,
            )

        if EXECUTION_SCHEMA != self.EXPECTED_MARKET_SCHEMA_VERSION:
            log_warn(
                self._logger,
                "Execution schema is behind market schema",
                execution_schema=EXECUTION_SCHEMA,
                expected_schema=self.EXPECTED_MARKET_SCHEMA_VERSION,
            )

        def check_handlers(domain: str, handlers: Mapping[str, Any]) -> None:
            for symbol, h in handlers.items():
                market = getattr(h, "market", None)
                schema = getattr(market, "schema_version", None)
                if schema is None:
                    log_warn(
                        self._logger,
                        "Handler market schema missing",
                        domain=domain,
                        symbol=symbol,
                    )
                    continue
                if int(schema) != self.EXPECTED_MARKET_SCHEMA_VERSION:
                    log_warn(
                        self._logger,
                        "Handler market schema mismatch",
                        domain=domain,
                        symbol=symbol,
                        handler_schema=int(schema),
                        expected_schema=self.EXPECTED_MARKET_SCHEMA_VERSION,
                    )

        check_handlers("ohlcv", self.ohlcv_handlers)
        check_handlers("orderbook", self.orderbook_handlers)
        check_handlers("option_chain", self.option_chain_handlers)
        check_handlers("iv_surface", self.iv_surface_handlers)
        check_handlers("sentiment", self.sentiment_handlers)
        check_handlers("trades", self.trades_handlers)
        check_handlers("option_trades", self.option_trades_handlers)

    def _require_methods(self, obj: object, methods: Iterable[str], *, label: str) -> None:
        missing = [name for name in methods if not callable(getattr(obj, name, None))]
        if missing:
            raise TypeError(f"{label} missing methods: {sorted(missing)}")

    def _validate_handler_contracts(self) -> None:
        if not self._guardrails_enabled:
            return
        required = (
            "load_history",
            "bootstrap",
            "warmup_to",
            "align_to",
            "last_timestamp",
            "get_snapshot",
            "window",
        )
        domains: dict[str, Mapping[str, RealTimeDataHandler]] = {
            "ohlcv": self.ohlcv_handlers,
            "orderbook": self.orderbook_handlers,
            "option_chain": self.option_chain_handlers,
            "iv_surface": self.iv_surface_handlers,
            "sentiment": self.sentiment_handlers,
            "trades": self.trades_handlers,
            "option_trades": self.option_trades_handlers,
        }
        for domain, handlers in domains.items():
            for symbol, handler in handlers.items():
                self._require_methods(
                    handler,
                    (*required, "on_new_tick"),
                    label=f"handler:{domain}:{symbol}",
                )

    def _validate_component_contracts(self) -> None:
        if not self._guardrails_enabled:
            return
        if not isinstance(self.models, Mapping):
            raise TypeError(f"models must be a mapping, got {type(self.models)!r}")
        for name, model in self.models.items():
            self._require_methods(model, ("predict",), label=f"model:{name}")
            if hasattr(model, "predict_with_context"):
                self._require_methods(model, ("predict_with_context",), label=f"model:{name}")
        self._require_methods(self.decision, ("decide",), label="decision")
        self._require_methods(self.risk_manager, ("adjust",), label="risk_manager")
        self._require_methods(self.execution_engine, ("execute",), label="execution_engine")
        self._require_methods(self.portfolio, ("apply_fill", "state"), label="portfolio")
        self._require_methods(self.feature_extractor, ("update",), label="feature_extractor")
        if hasattr(self.feature_extractor, "warmup"):
            self._require_methods(self.feature_extractor, ("warmup",), label="feature_extractor")
        if not hasattr(self.feature_extractor, "required_windows"):
            raise TypeError("feature_extractor missing required_windows")
        if not hasattr(self.feature_extractor, "warmup_steps"):
            raise TypeError("feature_extractor missing warmup_steps")
        required_windows = getattr(self.feature_extractor, "required_windows", None)
        if not isinstance(required_windows, dict):
            raise TypeError("feature_extractor.required_windows must be a dict")
        warmup_steps = getattr(self.feature_extractor, "warmup_steps", None)
        if not isinstance(warmup_steps, int) or warmup_steps <= 0:
            raise TypeError("feature_extractor.warmup_steps must be a positive int")

    def _get_price_ref(self, primary_snapshots: Mapping[str, Snapshot]) -> tuple[float, str, int | None] | None:
        orderbook = primary_snapshots.get("orderbook")
        if orderbook is not None:
            bid = orderbook.get_attr("best_bid") if hasattr(orderbook, "get_attr") else None
            ask = orderbook.get_attr("best_ask") if hasattr(orderbook, "get_attr") else None
            if bid is not None and ask is not None:
                try:
                    price = (float(bid) + float(ask)) / 2.0
                    data_ts = getattr(orderbook, "data_ts", None)
                    return price, "orderbook.mid", data_ts
                except (TypeError, ValueError):
                    pass
            mid = orderbook.get_attr("mid") if hasattr(orderbook, "get_attr") else None
            if mid is not None:
                try:
                    price = float(mid)
                    data_ts = getattr(orderbook, "data_ts", None)
                    return price, "orderbook.mid", data_ts
                except (TypeError, ValueError):
                    pass

        ohlcv = primary_snapshots.get("ohlcv")
        if ohlcv is not None:
            close = ohlcv.get_attr("close") if hasattr(ohlcv, "get_attr") else None
            if close is not None:
                try:
                    price = float(close)
                    data_ts = getattr(ohlcv, "data_ts", None)
                    return price, "ohlcv.close", data_ts
                except (TypeError, ValueError):
                    pass

        return None
        
    def _log_wiring(self) -> None:
        handler_types: dict[str, list[str]] = {}
        for domain, handlers in (
            ("ohlcv", self.ohlcv_handlers),
            ("orderbook", self.orderbook_handlers),
            ("option_chain", self.option_chain_handlers),
            ("iv_surface", self.iv_surface_handlers),
            ("sentiment", self.sentiment_handlers),
            ("trades", self.trades_handlers),
            ("option_trades", self.option_trades_handlers),
        ):
            if not handlers:
                continue
            types = sorted({type(h).__name__ for h in handlers.values()})
            handler_types[domain] = types
        model_types = {name: type(model).__name__ for name, model in self.models.items()}
        log_info(
            self._logger,
            "engine.wired",
            feature_extractor=type(self.feature_extractor).__name__,
            models=model_types,
            decision=type(self.decision).__name__,
            risk=type(self.risk_manager).__name__,
            execution=type(self.execution_engine).__name__,
            portfolio=type(self.portfolio).__name__,
            handlers=handler_types,
        )

    def _reset_step_order(self) -> None:
        self._step_stage_index = 0

    def _enter_stage(self, stage: str) -> None:
        if not self._guardrails_enabled:
            return
        if self._step_stage_index >= len(self.STEP_ORDER):
            raise FatalError(
                f"engine.step order overflow: {stage} after {self.STEP_ORDER[-1]}"
            )
        expected = self.STEP_ORDER[self._step_stage_index]
        if stage != expected:
            raise FatalError(
                f"engine.step order violation: expected {expected}, got {stage}"
            )
        self._step_stage_index += 1

    def ingest_tick(self, tick: Tick) -> None:
        """
        Relay ingestion of a single Tick from the Driver.
        StrategyEngine does not interpret time or ordering.
        """
        domain = getattr(tick, "domain", None)
        symbol = getattr(tick, "symbol", None)
        if domain == "ohlcv":
            try:
                ts = ensure_epoch_ms(tick.data_ts)
                key = format_tick_key(tick.domain, tick.symbol, getattr(tick, "source_id", None))
                count = int(getattr(self, "_ohlcv_key_log_count", 0))
                if count < 3: ## limit logs
                    log_info(
                        self._logger,
                        "ingest.ohlcv.key",
                        key=key,
                        symbol=tick.symbol,
                        source_id=getattr(tick, "source_id", None),
                        data_ts=getattr(tick, "data_ts", None),
                        tick_type=type(tick).__name__,
                    )
                    self._ohlcv_key_log_count = count + 1
                last = self._last_tick_ts_by_key.get(key)
                if last is None or int(ts) > int(last):
                    self._last_tick_ts_by_key[key] = int(ts)
            except Exception:
                pass
        if self._guardrails_enabled:
            try:
                ts = ensure_epoch_ms(tick.data_ts)
                key = format_tick_key(tick.domain, tick.symbol, getattr(tick, "source_id", None))
                last = self._last_tick_ts_by_key.get(key)
                assert_monotonic(ts, last, label=f"ingest:{key}")
                self._last_tick_ts_by_key[key] = int(ts)
            except Exception as exc:
                log_error(
                    self._logger,
                    "ingest.guard.failed",
                    domain=domain,
                    symbol=symbol,
                    tick_data_ts=getattr(tick, "data_ts", None),
                    err_type=type(exc).__name__,
                    err=str(exc),
                )
                raise FatalError(f"ingest_tick guard failed: {exc}") from exc
        domain = tick.domain
        payload = tick.payload

        domain_handlers = {
            "ohlcv": self.ohlcv_handlers,
            "orderbook": self.orderbook_handlers,
            "option_chain": self.option_chain_handlers,
            "iv_surface": self.iv_surface_handlers,
            "sentiment": self.sentiment_handlers,
            "trades": self.trades_handlers,
            "option_trades": self.option_trades_handlers,
        }


        handlers = domain_handlers.get(domain)
        if not handlers:
            key = f"{domain}:{symbol}"
            if key not in self._unhandled_domains_logged:
                self._unhandled_domains_logged.add(key)
                log_debug(
                    self._logger,
                    "ingest.domain.unhandled",
                    domain=domain,
                    symbol=symbol,
                )
            return
        assert symbol is not None
        handler = handlers.get(symbol)
        if domain == "ohlcv":
            if handler is None:
                key = f"{domain}:{symbol}"
                if key not in self._unhandled_symbols_logged:
                    self._unhandled_symbols_logged.add(key)
                    log_warn(
                        self._logger,
                        "ingest.symbol.unhandled",
                        domain=domain,
                        symbol=symbol,
                        handler_keys=list(handlers.keys()),
                    )
                return
            
        if handler is None: ### fuzzy match attempt -- only for non-ohlcv domains
            ### Some data sources may have different symbol conventions (e.g., with/without suffixes)
            for key in sorted(handlers.keys()):
                if symbol_matches(key, symbol):
                    handler = handlers.get(key)
                    count = int(getattr(self, "_fuzzy_route_count", 0))
                    if count < 3: ## limit logs
                        log_debug(
                            self._logger,
                            "engine.tick.routed_fuzzy",
                            domain=domain,
                            tick_symbol=symbol,
                            handler_symbol=key,
                            normalized_tick=normalize_symbol(symbol),
                            normalized_handler=normalize_symbol(key),
                        )
                    self._fuzzy_route_count = count + 1
                    break
            
        asset_domains = {"option_chain", "iv_surface", "option_trades", "sentiment"}
        if handler is None and domain in asset_domains:
            key = f"{domain}:{symbol}"
            if key not in self._unhandled_symbols_logged:
                self._unhandled_symbols_logged.add(key)
                log_warn(
                    self._logger,
                    "ingest.symbol.unhandled",
                    domain=domain,
                    symbol=symbol,
                    handler_keys=list(handlers.keys()),
                )
            return
        if handler is None and domain != "ohlcv":
            for h in handlers.values():
                aliases = getattr(h, "_symbol_aliases", None)
                if aliases is None:
                    aliases = {getattr(h, "symbol", None)}
                if symbol in aliases:
                    handler = h
                    break

        if handler is None:
            key = f"{domain}:{symbol}"
            if key not in self._unhandled_symbols_logged:
                self._unhandled_symbols_logged.add(key)
                log_debug(
                    self._logger,
                    "ingest.symbol.unhandled",
                    domain=domain,
                    symbol=symbol,
                )
            return
        

        expected_source = getattr(handler, "source_id", None)
        tick_source = getattr(tick, "source_id", None)
        
        if expected_source is not None and tick_source != expected_source:
            key = f"{domain}:{symbol}:{tick_source}"
            if key not in self._unhandled_sources_logged:
                self._unhandled_sources_logged.add(key)
                log_debug(
                    self._logger,
                    "ingest.source.unhandled",
                    domain=domain,
                    symbol=symbol,
                    source_id=tick_source,
                    handler_source_id=expected_source,
                )
            return
        if hasattr(handler, "on_new_tick"):
            handler.on_new_tick(tick)

    def align_to(self, ts: int) -> None:
        """
        Relay alignment to all handlers (anti-lookahead gate).
        """
        timestamp = ensure_epoch_ms(ts)
        self._anchor_ts = timestamp

        for hmap in (
            self.ohlcv_handlers,
            self.orderbook_handlers,
            self.option_chain_handlers,
            self.iv_surface_handlers,
            self.sentiment_handlers,
            self.trades_handlers,
            self.option_trades_handlers,
        ):
            for h in hmap.values():
                if hasattr(h, "align_to"):
                    h.align_to(timestamp)

        if self.mode in (EngineMode.REALTIME, EngineMode.MOCK):
            # Realtime/mock only: external backfill to close gaps before step().
            for hmap in (
                self.ohlcv_handlers,
                self.orderbook_handlers,
                self.option_chain_handlers,
                self.iv_surface_handlers,
                self.sentiment_handlers,
                self.trades_handlers,
                self.option_trades_handlers,
            ):
                for h in hmap.values():
                    maybe = getattr(h, "_maybe_backfill", None)
                    if callable(maybe):
                        maybe(target_ts=timestamp)

    def bootstrap(self, *, anchor_ts: int | None = None) -> None:
        """Realtime/mock bootstrap entrypoint (alias of preload_data)."""
        self.preload_data(anchor_ts=anchor_ts)

    def load_history(
        self,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> None:
        """Backtest entrypoint (alias of preload_data)."""
        anchor = start_ts if start_ts is not None else end_ts
        self.preload_data(anchor_ts=anchor)

    def get_snapshot(self) -> EngineSnapshot | None:
        return self.engine_snapshot

    def last_timestamp(self) -> int | None:
        return None if self._last_step_ts is None else int(self._last_step_ts)

    def iter_shutdown_objects(self) -> Iterable[object]:
        yield self
        for hmap in (
            self.ohlcv_handlers,
            self.orderbook_handlers,
            self.option_chain_handlers,
            self.iv_surface_handlers,
            self.sentiment_handlers,
            self.trades_handlers,
            self.option_trades_handlers,
        ):
            for h in hmap.values():
                yield h
        for obj in (
            self.feature_extractor,
            self.models,
            self.decision,
            self.risk_manager,
            self.execution_engine,
            self.portfolio,
        ):
            yield obj

    def _get_primary_ohlcv_handler(self):
        h = self.ohlcv_handlers.get(self.symbol)
        if h is None and self.ohlcv_handlers:
            h = next(iter(self.ohlcv_handlers.values()))
        return h

    def _extract_snapshot_ts(self, snap: Any) -> int | None:
        if isinstance(snap, Mapping):
            for key in ("data_ts", "event_ts", "timestamp", "ts"):
                if key in snap:
                    try:
                        return ensure_epoch_ms(snap[key])
                    except Exception:
                        return None
        return None

    def _check_snapshot_schema(self, label: str, snap: Any) -> None:
        if not self._guardrails_enabled:
            return
        if not isinstance(snap, Mapping):
            return
        if not snap:
            return
        cols = {str(k) for k in snap.keys()}
        expected = self._snapshot_schema_keys.get(label)
        if expected is None:
            self._snapshot_schema_keys[label] = cols
            return
        assert_schema_subset(cols, expected, label=label)
        assert_schema_subset(expected, cols, label=label)

    def _collect_market_data(self, ts: int) -> dict[str, dict[str, Any]]:
        market_data: dict[str, dict[str, Any]] = {}

        def add(domain: str, handlers: Mapping[str, Any]) -> None:
            if not handlers:
                return
            domain_map: dict[str, Any] = {}
            for sym, h in handlers.items():
                if not hasattr(h, "get_snapshot"):
                    continue
                try:
                    domain_map[str(sym)] = h.get_snapshot(ts)
                except TypeError:
                    domain_map[str(sym)] = h.get_snapshot()
                snap = domain_map[str(sym)]
                label = f"{domain}:{sym}"
                if snap is None or (isinstance(snap, Mapping) and not snap):
                    if label not in self._empty_snapshot_logged:
                        self._empty_snapshot_logged.add(label)
                        log_debug(
                            self._logger,
                            "market.snapshot.empty",
                            label=label,
                            ts=ts,
                        )
                self._check_snapshot_schema(label, snap)
                snap_ts = self._extract_snapshot_ts(snap)
                if snap_ts is not None:
                    assert_no_lookahead(ts, snap_ts, label=label)
            if domain_map:
                market_data[domain] = domain_map

        add("ohlcv", self.ohlcv_handlers)
        add("orderbook", self.orderbook_handlers)
        add("option_chain", self.option_chain_handlers)
        add("iv_surface", self.iv_surface_handlers)
        add("sentiment", self.sentiment_handlers)
        add("trades", self.trades_handlers)
        add("option_trades", self.option_trades_handlers)

        return market_data

    def preload_data(self, anchor_ts: int | None = None) -> None:
        """
        Preload data into handler caches (mode-agnostic).
        """
        if anchor_ts is not None:
            anchor_ts = ensure_epoch_ms(anchor_ts)
        required_windows = getattr(self.feature_extractor, "required_windows", None)
        if not isinstance(required_windows, dict) or not required_windows:
            raise RuntimeError(
                "preload_data() requires feature_extractor.required_windows "
                "(per-domain window dict)"
            )

        engine_interval_ms = getattr(self.spec, "interval_ms", None)
        log_info(
            self._logger,
            "engine.preload.start",
            anchor_ts=anchor_ts,
            required_windows=required_windows,
            engine_interval_ms=engine_interval_ms,
        )

        domain_handlers = {
            "ohlcv": self.ohlcv_handlers,
            "orderbook": self.orderbook_handlers,
            "option_chain": self.option_chain_handlers,
            "iv_surface": self.iv_surface_handlers,
            "sentiment": self.sentiment_handlers,
            "trades": self.trades_handlers,
            "option_trades": self.option_trades_handlers,
        }

        for domain, window in required_windows.items():
            if not isinstance(domain, str):
                raise ValueError(f"Invalid domain key in required_windows: {domain!r}")
            if not isinstance(window, int) or window <= 0:
                raise ValueError(
                    f"Invalid preload window for domain '{domain}': {window!r}"
                )
            handlers = domain_handlers.get(domain)
            if not handlers:
                log_warn(
                    self._logger,
                    "engine.preload.domain.missing_handlers",
                    domain=domain,
                )
                continue

            # Expand preload window to cover FeatureExtractor warmup steps.
            # If engine interval is coarser than a handler's interval, each warmup step
            # may advance multiple handler bars/snapshots.
            domain_interval_ms: int | None = None
            for hh in handlers.values():
                v = getattr(hh, "interval_ms", None)
                if isinstance(v, int) and v > 0:
                    domain_interval_ms = v
                    break

            step_mul = 1
            if isinstance(engine_interval_ms, int) and engine_interval_ms > 0 and isinstance(domain_interval_ms, int) and domain_interval_ms > 0:
                # ceil(engine / domain)
                step_mul = (engine_interval_ms + domain_interval_ms - 1) // domain_interval_ms

            expanded_window = int(window) + int(self.feature_extractor.warmup_steps) * int(step_mul)
            for h in handlers.values():
                if hasattr(h, "bootstrap"):
                    # Bootstrap is local-only. External backfill is handled separately.
                    log_debug(
                        self._logger,
                        "engine.preload.bootstrap",
                        domain=domain,
                        symbol=getattr(h, "symbol", None),
                        lookback=int(window),
                        expanded_window=int(expanded_window),
                        handler_interval_ms=domain_interval_ms,
                    )                    
                    h.bootstrap(anchor_ts=anchor_ts, lookback=expanded_window)

        self._preload_done = True
        self._anchor_ts = anchor_ts
        log_info(
            self._logger,
            "engine.preload.done",
            anchor_ts=anchor_ts,
        )

    # -------------------------------------------------
    def warmup_features(self, *, anchor_ts: int | None = None) -> None:
        """
        Warm up feature / model state using preloaded data.

        Semantics:
            - Feature-layer operation.
            - Assumes preload_data() has been called.
            - Does NOT load data.
            - Does NOT advance data cursors.
        """
        if not getattr(self, "_preload_done", False):
            raise RuntimeError("warmup_features() requires preload_data() first")

        # Resolve anchor_ts (only to define alignment point)
        if anchor_ts is None:
            candidates: list[int] = []
            for hmap in (
                self.ohlcv_handlers,
                self.orderbook_handlers,
                self.option_chain_handlers,
                self.iv_surface_handlers,
                self.sentiment_handlers,
                self.trades_handlers,
                self.option_trades_handlers,
            ):
                for h in hmap.values():
                    if hasattr(h, "last_timestamp"):
                        ts = h.last_timestamp()
                        if ts is not None:
                            candidates.append(int(ts))
            
            if not candidates:
                log_error(
                    self._logger,
                    "engine.warmup.no_anchor",
                )
                raise RuntimeError("Cannot infer anchor_ts for warmup")

            anchor_ts = min(candidates)
            log_debug(
                self._logger,
                "engine.warmup.infer_anchor",
                candidates_count=len(candidates),
                chosen_anchor_ts=anchor_ts,
            )

        anchor_ts = ensure_epoch_ms(anchor_ts)
        self._anchor_ts = anchor_ts

        log_info(
            self._logger,
            "engine.warmup.start",
            anchor_ts=anchor_ts,
        )

        required_windows = getattr(self.feature_extractor, "required_windows", None)
        if not isinstance(required_windows, dict) or not required_windows:
            raise RuntimeError(
                "warmup_features() requires feature_extractor.required_windows "
                "(per-domain window dict)"
            )
        domain_handlers = {
            "ohlcv": self.ohlcv_handlers,
            "orderbook": self.orderbook_handlers,
            "option_chain": self.option_chain_handlers,
            "iv_surface": self.iv_surface_handlers,
            "sentiment": self.sentiment_handlers,
            "trades": self.trades_handlers,
            "option_trades": self.option_trades_handlers,
        }

        def _window_count(win: Any) -> int:
            if win is None:
                return 0
            try:
                return int(len(win))
            except Exception:
                try:
                    return int(len(list(win)))
                except Exception:
                    return 0

        def _has_required_history(handler: Any, required: int) -> bool:
            if required <= 0:
                return True
            if not hasattr(handler, "window"):
                return False
            try:
                win = handler.window(anchor_ts, required)
            except Exception:
                return False
            return _window_count(win) >= int(required)

        missing: list[tuple[str, str, Any, int]] = []
        for domain, window in required_windows.items():
            if not isinstance(domain, str):
                continue
            if not isinstance(window, int) or window <= 0:
                continue
            handlers = domain_handlers.get(domain)
            if not handlers:
                continue
            for sym, h in handlers.items():
                if not _has_required_history(h, int(window)):
                    missing.append((domain, str(sym), h, int(window)))

        if missing:
            missing_labels = [f"{d}:{s}" for d, s, _, _ in missing]
            if self.mode == EngineMode.BACKTEST:
                raise RuntimeError(
                    f"warmup_features() missing history for: {missing_labels}"
                )
            log_warn(
                self._logger,
                "engine.warmup.missing_history",
                missing=missing_labels,
            )
            for domain, sym, h, window in missing:
                interval_ms = getattr(h, "interval_ms", None)
                if not isinstance(interval_ms, int) or interval_ms <= 0:
                    log_warn(
                        self._logger,
                        "engine.warmup.backfill.no_interval",
                        domain=domain,
                        symbol=sym,
                    )
                    continue
                backfill = getattr(h, "_backfill_from_source", None)
                if not callable(backfill):
                    log_warn(
                        self._logger,
                        "engine.warmup.backfill.no_source",
                        domain=domain,
                        symbol=sym,
                    )
                    continue
                start_ts = int(anchor_ts) - (int(window) - 1) * int(interval_ms)
                if start_ts < 0:
                    start_ts = 0
                log_warn(
                    self._logger,
                    "engine.warmup.backfill.start",
                    domain=domain,
                    symbol=sym,
                    start_ts=int(start_ts),
                    end_ts=int(anchor_ts),
                )
                backfill(start_ts=int(start_ts), end_ts=int(anchor_ts), target_ts=int(anchor_ts))

            still_missing = [
                (d, s, h, w)
                for d, s, h, w in missing
                if not _has_required_history(h, int(w))
            ]
            if still_missing:
                still_labels = [f"{d}:{s}" for d, s, _, _ in still_missing]
                raise RuntimeError(
                    f"warmup_features() insufficient history after backfill: {still_labels}"
                )

        # IMPORTANT:
        # StrategyEngine does NOT loop history.
        # It delegates warmup to FeatureExtractor.
        
        if hasattr(self.feature_extractor, "warmup"):
            self.feature_extractor.warmup(anchor_ts=anchor_ts)
        else:
            # fallback: at least initialize features at anchor
            self.feature_extractor.update(timestamp=anchor_ts)

        self._warmup_done = True

        log_info(
            self._logger,
            "engine.warmup.done",
            anchor_ts=anchor_ts,
        )

    # -------------------------------------------------
    # Bootstrap / backfill phase (realtime/mock only)
    # -------------------------------------------------


    def step(self, *, ts: int) -> EngineSnapshot:
        """
        Execute a single strategy step at an explicit timestamp.

        Semantics:
            - `ts` is provided by the driver (primary clock).
            - Driver must have aligned all data handlers to ts before calling step().
            - No handler is allowed to infer or advance time internally.
        """
        if not getattr(self, "_warmup_done", False):
            raise RuntimeError(
                "StrategyEngine.step() called before warmup_features(). "
                "Call engine.preload_data() then engine.warmup_features() first."
            )
        if self.mode == EngineMode.BACKTEST and not getattr(self, "_preload_done", False):
            raise RuntimeError("BACKTEST step() called before preload_data()")

        self._reset_step_order()
        timestamp = ensure_epoch_ms(ts)
        if self._guardrails_enabled:
            assert_monotonic(timestamp, self._last_step_ts, label="engine.step")
            self._last_step_ts = int(timestamp)
        self._anchor_ts = timestamp  # keep last alignment point

        log_debug(self._logger, "StrategyEngine step() called", timestamp=timestamp)

        # -------------------------------------------------
        # 1. Pull current market snapshot (primary clock source)
        # -------------------------------------------------
        self._enter_stage("handlers")
        market_snapshots = self._collect_market_data(timestamp)
        primary_symbol = self.symbol

        ##### Soft readiness context, sometimes async has jammed data or in realtime lagging data #####
        readiness_ctx: dict[str, dict[str, dict[str, Any]]] = {}
        soft_cfg = self.universe.get("soft_readiness") or self.universe.get("SOFT_READINESS") or {}
        max_staleness_ms = None
        if isinstance(soft_cfg, dict) and bool(soft_cfg.get("enabled", False)):
            max_staleness_ms = soft_cfg.get("max_staleness_ms")
            domains = soft_cfg.get("domains")
            domain_handlers = {
                "orderbook": self.orderbook_handlers,
                "option_chain": self.option_chain_handlers,
                "iv_surface": self.iv_surface_handlers,
                "sentiment": self.sentiment_handlers,
                "trades": self.trades_handlers,
                "option_trades": self.option_trades_handlers,
            }
            if isinstance(domains, (list, tuple, set)):
                domain_list = [str(d) for d in domains if d]
            else:
                domain_list = list(domain_handlers.keys())
            for domain in domain_list:
                handlers = domain_handlers.get(domain)
                if not handlers:
                    continue
                symbol_ctx: dict[str, dict[str, Any]] = {}
                for symbol, handler in handlers.items():
                    last_ts = None
                    if hasattr(handler, "last_timestamp"):
                        try:
                            last_ts = handler.last_timestamp()
                        except Exception:
                            last_ts = None
                    exists = last_ts is not None and int(last_ts) <= int(timestamp)
                    staleness_ms = int(timestamp) - int(last_ts) if last_ts is not None else None
                    symbol_ctx[str(symbol)] = {
                        "last_ts": last_ts,
                        "staleness_ms": staleness_ms,
                        "exists": exists,
                    }
                if symbol_ctx:
                    readiness_ctx[domain] = symbol_ctx
        ##### Soft readiness context, sometimes async has jammed data or in realtime lagging data #####

        # primary_snapshots is per-domain for the primary symbol (no OHLCV-as-canonical shortcut).
        primary_snapshots = {
            domain: snaps[primary_symbol]
            for domain, snaps in market_snapshots.items()
            if isinstance(snaps, Mapping) and primary_symbol in snaps
        }

        if self._guardrails_enabled:
            for domain, snap in primary_snapshots.items():
                label = f"primary:{domain}:{primary_symbol}"
                self._check_snapshot_schema(label, snap)
                snap_ts = self._extract_snapshot_ts(snap)
                if snap_ts is not None:
                    assert_no_lookahead(timestamp, snap_ts, label=label)

        # -------------------------------------------------
        # 2. Feature computation (v4 snapshot-based)
        # -------------------------------------------------
        self._enter_stage("features")
        features = self.feature_extractor.update(timestamp=timestamp)
        log_debug(
            self._logger,
            "StrategyEngine computed features",
            feature_keys=list(features.keys()),
        )

        # Use ALL features — model decides what to use (v4 contract)
        filtered_features = features

        # -------------------------------------------------
        # 3. Portfolio snapshot (used by model/decision/risk)
        # -------------------------------------------------
        if hasattr(self.portfolio, "update_marks"):
            self.portfolio.update_marks(market_snapshots)
        portfolio_state = self.portfolio.state()
        if hasattr(portfolio_state, "to_dict"):
            portfolio_state_dict = dict(portfolio_state.to_dict())
        elif isinstance(portfolio_state, Mapping):
            portfolio_state_dict = dict(portfolio_state)
        else:
            portfolio_state_dict = {"state": portfolio_state}

        # -------------------------------------------------
        # 4. Model predictions
        # -------------------------------------------------
        self._enter_stage("models")
        model_outputs: dict[str, Any] = {}
        model_context = {
            "timestamp": timestamp,
            "features": filtered_features,
            "portfolio": portfolio_state_dict,
            "primary_snapshots": primary_snapshots,
            "market_snapshots": market_snapshots,
        }
        for name, model in self.models.items():
            if hasattr(model, "predict_with_context"):
                model_outputs[name] = model.predict_with_context( 
                    filtered_features,
                    model_context,
                ) 
            else:
                model_outputs[name] = model.predict(filtered_features)
        log_debug(self._logger, "StrategyEngine model outputs", outputs=model_outputs)

        # -------------------------------------------------
        # 5. Construct decision context
        # -------------------------------------------------

        context = {
            "timestamp": timestamp,
            "features": filtered_features,
            "models": model_outputs,
            "portfolio": portfolio_state_dict,
            "primary_snapshots": primary_snapshots,
            "market_snapshots": market_snapshots,
            "readiness_ctx": readiness_ctx,
            "soft_readiness_max_staleness_ms": max_staleness_ms,
        }

        price_ref = self._get_price_ref(primary_snapshots)
        if price_ref is not None:
            price_value, price_source, price_data_ts = price_ref
            log_debug(
                self._logger,
                "decision.price_trace",
                symbol=self.symbol,
                engine_ts=timestamp,
                price_ref=price_value,
                price_source=price_source,
                price_data_ts=price_data_ts,
            )

        # DecisionProto.decide(context) → score
        self._enter_stage("decision")
        decision_score = self.decision.decide(context)
        log_debug(self._logger, "StrategyEngine decision score", score=decision_score)
        context["decision_score"] = decision_score

        # -------------------------------------------------
        # 6. Risk: convert score to target position
        # -------------------------------------------------
        size_intent = decision_score
        self._enter_stage("risk")
        target_position = self.risk_manager.adjust(size_intent, context)
        log_debug(
            self._logger,
            "StrategyEngine risk target",
            target_position=target_position,
        )

        # -------------------------------------------------
        # 7. Execution Pipeline
        # -------------------------------------------------
        self._enter_stage("execution")
        fills = self.execution_engine.execute(
            target_position=target_position,
            portfolio_state=context["portfolio"],
            primary_snapshots=primary_snapshots,
            timestamp=timestamp,
        )
        log_debug(self._logger, "StrategyEngine execution fills", fills=fills)

        # -------------------------------------------------
        # 8. Apply fills to portfolio
        # -------------------------------------------------
        self._enter_stage("portfolio")
        execution_outcomes: list[dict[str, Any]] = []
        for f in fills:
            outcome = self.portfolio.apply_fill(f)
            if isinstance(outcome, dict) and outcome:
                execution_outcomes.append(outcome)
        # -------------------------------------------------
        # 9. Return immutable engine snapshot (post-execution)
        # -------------------------------------------------
        self._enter_stage("snapshot")
        features_out = dict(features) if isinstance(features, Mapping) else {"features": features}
        model_outputs_out = dict(model_outputs)
        fills_out = list(fills)
        if hasattr(self.portfolio, "update_marks"):
            self.portfolio.update_marks(market_snapshots)
        portfolio_post = self.portfolio.state()
        if isinstance(portfolio_post, PortfolioState):
            portfolio_post = PortfolioState(dict(portfolio_post.to_dict()))
        snapshot = EngineSnapshot(
                    timestamp=timestamp,
                    mode=self.spec.mode,                 # engine-owned
                    features=features_out,
                    model_outputs=model_outputs_out,
                    decision_score=decision_score,
                    target_position=target_position,
                    fills=fills_out,
                    portfolio=portfolio_post,    # post-fill
                )
        self.engine_snapshot = snapshot
        log_debug(self._logger, "StrategyEngine snapshot ready")

        expected_visible_end_ts = None
        actual_last_ts = None
        closed_bar_ready = None
        if self.mode == EngineMode.BACKTEST:
            handler = self._get_primary_ohlcv_handler()
            interval_ms = getattr(handler, "interval_ms", None) if handler is not None else None
            if handler is not None and isinstance(interval_ms, int) and interval_ms > 0:
                expected_visible_end_ts = visible_end_ts(int(timestamp), int(interval_ms))
                if callable(getattr(handler, "last_timestamp", None)):
                    actual_last_ts = handler.last_timestamp()
                    if actual_last_ts is not None:
                        closed_bar_ready = int(actual_last_ts) >= int(expected_visible_end_ts)

        log_step_trace(
            self._logger,
            step_ts=timestamp,
            strategy=self.strategy_name,
            symbol=self.symbol,
            features=features,
            models=model_outputs,
            portfolio=portfolio_post,
            primary_snapshots=None,
            market_snapshots=market_snapshots,
            decision_score=decision_score,
            target_position=target_position,
            fills=fills,
            execution_outcomes=execution_outcomes or None,
            guardrails={
                "last_step_ts": self._last_step_ts,
                "last_tick_ts_by_key": self._last_tick_ts_by_key,
                "step_stage_index": self._step_stage_index,
            },
            expected_visible_end_ts=expected_visible_end_ts,
            actual_last_ts=int(actual_last_ts) if actual_last_ts is not None else None,
            closed_bar_ready=closed_bar_ready,
        )

        if self._guardrails_enabled and self._step_stage_index != len(self.STEP_ORDER):
            raise FatalError(
                f"engine.step order incomplete: {self._step_stage_index}/{len(self.STEP_ORDER)}"
            )

        return snapshot
