from __future__ import annotations

from typing import Any
from collections.abc import Mapping
from enum import Enum
from dataclasses import dataclass

from quant_engine.data.derivatives.iv.iv_handler import IVSurfaceDataHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.data.orderbook.realtime import RealTimeOrderbookHandler
from quant_engine.data.sentiment.sentiment_handler import SentimentHandler
from quant_engine.features.extractor import FeatureExtractor
from quant_engine.utils.logger import get_logger, log_debug


class EngineMode(Enum):
    REALTIME = "realtime"
    BACKTEST = "backtest"
    MOCK = "mock"


# EngineSpec dataclass for encapsulating engine configuration
@dataclass(frozen=True)
class EngineSpec:
    mode: EngineMode
    interval: str # e.g. "1m", "5m"
    symbol: str
    universe: dict[str, Any] | None = None

class StrategyEngine:
    """Orchestrator of the entire quant pipeline.

    It does not compute features or model predictions itself;
    it only coordinates each Layer.
    """

    _logger = get_logger(__name__)

    def __init__(
        self,
        *,
        spec: EngineSpec,
        ohlcv_handlers: Mapping[str, OHLCVDataHandler],          # dict[str, RealTimeDataHandler or HistoricalDataHandler]
        orderbook_handlers: Mapping[str, RealTimeOrderbookHandler],      # dict[str, RealTimeOrderbookHandler or HistoricalOrderbookHandler]
        option_chain_handlers: Mapping[str, OptionChainDataHandler],   # dict[str, OptionChainDataHandler]
        iv_surface_handlers: Mapping[str, IVSurfaceDataHandler],     # dict[str, IVSurfaceDataHandler]
        sentiment_handlers: Mapping[str, SentimentHandler],      # dict[str, SentimentHandler]
        feature_extractor: FeatureExtractor,
        models,
        decision,
        risk_manager,
        execution_engine,
        portfolio_manager
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
        self.feature_extractor = feature_extractor
        self.models = models
        self.decision = decision
        self.risk_manager = risk_manager
        self.execution_engine = execution_engine
        self.portfolio = portfolio_manager
        log_debug(self._logger, "StrategyEngine initialized",
                  mode=self.spec.mode.value,
                  model_count=len(models))

    def _get_primary_ohlcv_handler(self):
        h = self.ohlcv_handlers.get(self.symbol)
        if h is None and self.ohlcv_handlers:
            h = next(iter(self.ohlcv_handlers.values()))
        return h

    def preload_data(self) -> None:
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

            for h in handlers.values():
                if hasattr(h, "bootstrap"):
                    h.bootstrap(required_window=window)

        self._preload_done = True

        log_debug(
            self._logger,
            "StrategyEngine preload_data completed",
            required_windows=required_windows,
        )

    # -------------------------------------------------
    def warmup_features(self, *, anchor_ts: float | None = None) -> None:
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
            candidates: list[float] = []
            for hmap in (
                self.ohlcv_handlers,
                self.orderbook_handlers,
                self.option_chain_handlers,
                self.iv_surface_handlers,
                self.sentiment_handlers,
            ):
                for h in hmap.values():
                    if hasattr(h, "last_timestamp"):
                        ts = h.last_timestamp()
                        if ts is not None:
                            candidates.append(ts)

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


    def step(self, *, ts: float) -> dict[str, Any]:
        """
        Execute a single strategy step at an explicit timestamp.

        Semantics:
            - `ts` is provided by the driver (primary clock).
            - All data handlers are aligned (anti-lookahead) to `ts`.
            - No handler is allowed to infer or advance time internally.
        """
        if not getattr(self, "_warmup_done", False):
            raise RuntimeError(
                "StrategyEngine.step() called before warmup_features(). "
                "Call engine.preload_data() then engine.warmup_features() first."
            )
        if self.mode == EngineMode.BACKTEST and not getattr(self, "_preload_done", False):
            raise RuntimeError("BACKTEST step() called before preload_data()")

        timestamp = float(ts)
        self._anchor_ts = timestamp  # keep last alignment point

        log_debug(self._logger, "StrategyEngine step() called", timestamp=timestamp)

        # -------------------------------------------------
        # 0. Align all handlers to this timestamp (anti-lookahead clamp)
        # -------------------------------------------------
        for hmap in (
            self.ohlcv_handlers,
            self.orderbook_handlers,
            self.option_chain_handlers,
            self.iv_surface_handlers,
            self.sentiment_handlers,
        ):
            for h in hmap.values():
                if hasattr(h, "align_to"):
                    h.align_to(timestamp)

        # -------------------------------------------------
        # 1. Pull current market snapshot (primary clock source)
        # -------------------------------------------------
        primary_handler = self._get_primary_ohlcv_handler()
        market_data: Any = None
        if primary_handler is not None and hasattr(primary_handler, "get_snapshot"):
            try:
                market_data = primary_handler.get_snapshot(timestamp)
            except TypeError:
                # backward compatibility: older handlers may not accept ts
                market_data = primary_handler.get_snapshot()

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
        # 3. Model predictions
        # -------------------------------------------------
        model_outputs: dict[str, Any] = {}
        for name, model in self.models.items():
            model_outputs[name] = model.predict(filtered_features)
        log_debug(self._logger, "StrategyEngine model outputs", outputs=model_outputs)

        # -------------------------------------------------
        # 4. Construct decision context
        # -------------------------------------------------
        portfolio_state = self.portfolio.state()
        if hasattr(portfolio_state, "to_dict"):
            portfolio_state_dict = portfolio_state.to_dict()
        elif isinstance(portfolio_state, dict):
            portfolio_state_dict = portfolio_state
        else:
            portfolio_state_dict = dict(portfolio_state)

        context = {
            "timestamp": timestamp,
            "features": filtered_features,
            "models": model_outputs,
            "portfolio": portfolio_state_dict,
            "market_data": market_data,
        }

        # DecisionProto.decide(context) → score
        decision_score = self.decision.decide(context)
        log_debug(self._logger, "StrategyEngine decision score", score=decision_score)

        # -------------------------------------------------
        # 5. Risk: convert score to target position
        # -------------------------------------------------
        size_intent = decision_score
        target_position = self.risk_manager.adjust(size_intent, context)
        log_debug(
            self._logger,
            "StrategyEngine risk target",
            target_position=target_position,
        )

        # -------------------------------------------------
        # 6. Execution Pipeline
        # -------------------------------------------------
        fills = self.execution_engine.execute(
            target_position=target_position,
            portfolio_state=context["portfolio"],
            market_data=market_data,
            timestamp=timestamp,
        )
        log_debug(self._logger, "StrategyEngine execution fills", fills=fills)

        # -------------------------------------------------
        # 7. Apply fills to portfolio
        # -------------------------------------------------
        for f in fills:
            self.portfolio.apply_fill(f)

        # -------------------------------------------------
        # 8. Return current strategy snapshot
        # -------------------------------------------------
        snapshot = {
            "timestamp": timestamp,
            "context": context,
            "decision_score": decision_score,
            "target_position": target_position,
            "market_data": market_data,
            "fills": fills,
        }
        log_debug(self._logger, "StrategyEngine snapshot ready")

        return snapshot