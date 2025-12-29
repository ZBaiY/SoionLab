from __future__ import annotations

from typing import Any
from collections.abc import Mapping
from enum import Enum
from dataclasses import dataclass
from quant_engine.contracts.decision import DecisionBase
from quant_engine.contracts.model import ModelBase
from quant_engine.execution.engine import ExecutionEngine
from quant_engine.risk.engine import RiskEngine
from quant_engine.runtime.snapshot import EngineSnapshot, SCHEMA_VERSION as RUNTIME_SNAPSHOT_SCHEMA
from quant_engine.runtime.context import SCHEMA_VERSION as RUNTIME_CONTEXT_SCHEMA
from quant_engine.contracts.decision import SCHEMA_VERSION as DECISION_SCHEMA
from quant_engine.contracts.risk import SCHEMA_VERSION as RISK_SCHEMA
from quant_engine.execution.engine import SCHEMA_VERSION as EXECUTION_SCHEMA
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler
from quant_engine.data.derivatives.iv.iv_handler import IVSurfaceDataHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.data.orderbook.realtime import RealTimeOrderbookHandler
from quant_engine.data.sentiment.sentiment_handler import SentimentDataHandler
from quant_engine.data.trades.realtime import TradesDataHandler
from quant_engine.data.derivatives.option_trades.realtime import OptionTradesDataHandler
from quant_engine.features.extractor import FeatureExtractor
from quant_engine.contracts.portfolio import PortfolioBase
from quant_engine.utils.logger import get_logger, log_debug, log_warn
from ingestion.contracts.tick import IngestionTick as Tick


class StrategyEngine:
    """Orchestrator of the entire quant pipeline.

    It does not compute features or model predictions itself;
    it only coordinates each Layer.
    """

    _logger = get_logger(__name__)
    LAYER_SCHEMA_VERSION = 1
    EXPECTED_MARKET_SCHEMA_VERSION = 2

    def __init__(
        self,
        *,
        spec: EngineSpec,
        ohlcv_handlers: Mapping[str, OHLCVDataHandler],          # dict[str, RealTimeDataHandler or HistoricalDataHandler]
        orderbook_handlers: Mapping[str, RealTimeOrderbookHandler],      # dict[str, RealTimeOrderbookHandler or HistoricalOrderbookHandler]
        option_chain_handlers: Mapping[str, OptionChainDataHandler],   # dict[str, OptionChainDataHandler]
        iv_surface_handlers: Mapping[str, IVSurfaceDataHandler],     # dict[str, IVSurfaceDataHandler]
        sentiment_handlers: Mapping[str, SentimentDataHandler],      # dict[str, SentimentHandler]
        trades_handlers: Mapping[str, TradesDataHandler],
        option_trades_handlers: Mapping[str, OptionTradesDataHandler],
        feature_extractor: FeatureExtractor,
        models,
        decision,
        risk_manager,
        execution_engine,
        portfolio_manager: PortfolioBase,
    ):
        self.spec = spec
        self.mode = spec.mode
        self.symbol = spec.symbol
        self.universe = spec.universe or {}
        self.ohlcv_handlers = ohlcv_handlers
        self.orderbook_handlers = orderbook_handlers
        self.option_chain_handlers = option_chain_handlers
        self.iv_surface_handlers = iv_surface_handlers
        self.sentiment_handlers = sentiment_handlers
        self.trades_handlers = trades_handlers
        self.option_trades_handlers = option_trades_handlers
        self.feature_extractor = feature_extractor
        self.models = models
        self.decision = decision
        self.risk_manager = risk_manager
        self.execution_engine = execution_engine
        self.portfolio = portfolio_manager
        self.engine_snapshot = EngineSnapshot(0, self.mode, {}, {}, None, None, [], None, portfolio_manager.state())
        log_debug(self._logger, "StrategyEngine initialized",
                  mode=self.spec.mode.value,
                  model_count=len(models))
        self._warn_schema_mismatches()

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

    def ingest_tick(self, tick: Tick) -> None:
        """
        Relay ingestion of a single Tick from the Driver.
        StrategyEngine does not interpret time or ordering.
        """
        domain = tick.domain
        payload = tick.payload

        domain_handlers = {
            "ohlcv": self.ohlcv_handlers,
            "orderbook": self.orderbook_handlers,
            "option_chain": self.option_chain_handlers,
            "iv_surface": self.iv_surface_handlers,
            "sentiment": self.sentiment_handlers,
            "trades": self.trades_handlers,
            "trade": self.trades_handlers,
            "option_trades": self.option_trades_handlers,
            "option_trade": self.option_trades_handlers,
        }

        handlers = domain_handlers.get(domain)
        if not handlers:
            return

        for h in handlers.values():
            if hasattr(h, "on_new_tick"):
                h.on_new_tick(payload)

    def align_to(self, ts: int) -> None:
        """
        Relay alignment to all handlers (anti-lookahead gate).
        """
        timestamp = int(ts)
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

    def _get_primary_ohlcv_handler(self):
        h = self.ohlcv_handlers.get(self.symbol)
        if h is None and self.ohlcv_handlers:
            h = next(iter(self.ohlcv_handlers.values()))
        return h

    def _get_primary_market_snapshot(self, ts: int) -> Any:
        primary_handler = self._get_primary_ohlcv_handler()
        if primary_handler is not None and hasattr(primary_handler, "get_snapshot"):
            try:
                return primary_handler.get_snapshot(ts)
            except TypeError:
                return primary_handler.get_snapshot()
        return None

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
        required_windows = getattr(self.feature_extractor, "required_windows", None)
        if not isinstance(required_windows, dict) or not required_windows:
            raise RuntimeError(
                "preload_data() requires feature_extractor.required_windows "
                "(per-domain window dict)"
            )

        log_debug(
            self._logger,
            "StrategyEngine preload_data started",
            required_windows=required_windows,
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
                continue

            # Expand preload window to cover FeatureExtractor warmup steps.
            # If engine interval is coarser than a handler's interval, each warmup step
            # may advance multiple handler bars/snapshots.
            engine_interval_ms = getattr(self.spec, "interval_ms", None)

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
                    assert isinstance(h, RealTimeDataHandler)
                    h.bootstrap(anchor_ts=anchor_ts, lookback=expanded_window)

        self._preload_done = True
        self._anchor_ts = anchor_ts
        log_debug(
            self._logger,
            "StrategyEngine preload_data completed",
            required_windows=required_windows,
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
                raise RuntimeError("Cannot infer anchor_ts for warmup")

            anchor_ts = min(candidates)

        self._anchor_ts = anchor_ts

        log_debug(
            self._logger,
            "StrategyEngine warmup_features started",
            anchor_ts=anchor_ts,
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

        log_debug(
            self._logger,
            "StrategyEngine warmup_features completed",
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

        timestamp = int(ts)
        self._anchor_ts = timestamp  # keep last alignment point

        log_debug(self._logger, "StrategyEngine step() called", timestamp=timestamp)

        # -------------------------------------------------
        # 1. Pull current market snapshot (primary clock source)
        # -------------------------------------------------
        market_snapshots = self._collect_market_data(timestamp)
        market_data = self._get_primary_market_snapshot(timestamp)

        # -------------------------------------------------
        # 2. Feature computation (v4 snapshot-based)
        # -------------------------------------------------
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
        portfolio_state = self.portfolio.state()
        if hasattr(portfolio_state, "to_dict"):
            portfolio_state_dict = portfolio_state.to_dict()
        elif isinstance(portfolio_state, dict):
            portfolio_state_dict = portfolio_state
        else:
            portfolio_state_dict = {"state": portfolio_state}

        # -------------------------------------------------
        # 4. Model predictions
        # -------------------------------------------------
        model_outputs: dict[str, Any] = {}
        model_context = {
            "timestamp": timestamp,
            "features": filtered_features,
            "portfolio": portfolio_state_dict,
            "market_data": market_data,
            "market_snapshots": market_snapshots,
        }
        for name, model in self.models.items():
            assert isinstance(model, ModelBase)
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
            "market_data": market_data,
            "market_snapshots": market_snapshots,
        }

        # DecisionProto.decide(context) → score
        assert isinstance(self.decision, DecisionBase)
        decision_score = self.decision.decide(context)
        log_debug(self._logger, "StrategyEngine decision score", score=decision_score)

        # -------------------------------------------------
        # 6. Risk: convert score to target position
        # -------------------------------------------------
        size_intent = decision_score
        assert isinstance(self.risk_manager, RiskEngine)
        target_position = self.risk_manager.adjust(size_intent, context)
        log_debug(
            self._logger,
            "StrategyEngine risk target",
            target_position=target_position,
        )

        # -------------------------------------------------
        # 7. Execution Pipeline
        # -------------------------------------------------
        assert isinstance(self.execution_engine, ExecutionEngine)
        fills = self.execution_engine.execute(
            target_position=target_position,
            portfolio_state=context["portfolio"],
            market_data=market_data,
            timestamp=timestamp,
        )
        log_debug(self._logger, "StrategyEngine execution fills", fills=fills)

        # -------------------------------------------------
        # 8. Apply fills to portfolio
        # -------------------------------------------------
        for f in fills:
            self.portfolio.apply_fill(f)

        # -------------------------------------------------
        # 9. Return immutable engine snapshot (post-execution)
        # -------------------------------------------------
        snapshot = EngineSnapshot(
                    timestamp=timestamp,
                    mode=self.spec.mode,                 # engine-owned
                    features=features,
                    model_outputs=model_outputs,
                    decision_score=decision_score,
                    target_position=target_position,
                    fills=fills,
                    market_data=market_snapshots,
                    portfolio=self.portfolio.state(),    # post-fill
                )
        self.engine_snapshot = snapshot
        log_debug(self._logger, "StrategyEngine snapshot ready")

        return snapshot
