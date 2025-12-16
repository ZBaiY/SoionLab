from __future__ import annotations

from typing import Any
from collections.abc import Mapping
from enum import Enum

from quant_engine.data.derivatives.iv.iv_handler import IVSurfaceDataHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.ohlcv.realtime import RealTimeDataHandler
from quant_engine.data.orderbook.realtime import RealTimeOrderbookHandler
from quant_engine.data.sentiment.loader import SentimentLoader
from quant_engine.features.extractor import FeatureExtractor
from quant_engine.utils.logger import get_logger, log_debug


class EngineMode(Enum):
    REALTIME = "realtime"
    BACKTEST = "backtest"
    MOCK = "mock"

class StrategyEngine:
    """Orchestrator of the entire quant pipeline.

    It does not compute features or model predictions itself;
    it only coordinates each Layer.
    """

    _logger = get_logger(__name__)

    def __init__(
        self,
        *,
        mode: EngineMode,
        symbol: str,
        ohlcv_handlers: Mapping[str, RealTimeDataHandler],          # dict[str, RealTimeDataHandler or HistoricalDataHandler]
        orderbook_handlers: Mapping[str, RealTimeOrderbookHandler],      # dict[str, RealTimeOrderbookHandler or HistoricalOrderbookHandler]
        option_chain_handlers: Mapping[str, OptionChainDataHandler],   # dict[str, OptionChainDataHandler]
        iv_surface_handlers: Mapping[str, IVSurfaceDataHandler],     # dict[str, IVSurfaceDataHandler]
        sentiment_handlers: Mapping[str, SentimentLoader],      # dict[str, SentimentLoader]
        feature_extractor: FeatureExtractor,
        models,
        decision,
        risk_manager,
        execution_engine,
        portfolio_manager
    ):
        self.mode = mode
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
        self.symbol = symbol
        log_debug(self._logger, "StrategyEngine initialized",
                  mode=self.mode.value,
                  model_count=len(models))

    def _get_primary_ohlcv_handler(self):
        h = self.ohlcv_handlers.get(self.symbol)
        if h is None and self.ohlcv_handlers:
            h = next(iter(self.ohlcv_handlers.values()))
        return h
 
    # -------------------------------------------------
    # Historical data loading phase (backtest only)
    # -------------------------------------------------
    def load_history(
        self,
        *,
        start_ts: float,
        end_ts: float | None = None,
    ) -> None:
        """
        Load historical data into data handlers.

        This phase:
        - populates handler caches
        - does NOT compute features
        - does NOT place orders

        Must be called before warmup() in backtest mode.
        """
        if self.mode != EngineMode.BACKTEST:
            raise RuntimeError(
                "load_history() is only valid in BACKTEST mode"
            )

        log_debug(self._logger, "StrategyEngine load_history started",
                  start_ts=start_ts, end_ts=end_ts)

        self._history_start_ts = start_ts
        self._history_end_ts = end_ts

        for hmap in (
            self.ohlcv_handlers,
            self.orderbook_handlers,
            self.option_chain_handlers,
            self.iv_surface_handlers,
            self.sentiment_handlers,
        ):
            for h in hmap.values():
                if hasattr(h, "load_history"):
                    h.load_history(start_ts=start_ts, end_ts=end_ts)

        log_debug(self._logger, "StrategyEngine load_history completed")

    # -------------------------------------------------
    # Initialization / warm-up phase (backtest & live)
    # -------------------------------------------------
    def warmup(
        self,
        *,
        anchor_ts: float | None = None,
        warmup_steps: int = 0,
    ) -> None:
        """
        Prepare the strategy for execution by aligning all handlers
        to a common anchor timestamp and warming up feature caches.

        This phase:
        - assumes historical data is already loaded (backtest) OR
          real-time handlers are already streaming (live)
        - does NOT load data
        - does NOT place orders
        """
        if self.mode == EngineMode.REALTIME:
            log_debug(self._logger, "Warmup in REALTIME mode")

        if hasattr(self, "_history_start_ts"):
            log_debug(self._logger, "Warmup using preloaded historical data")

        log_debug(self._logger, "StrategyEngine warmup started")

        # -------------------------------------------------
        # 1. Resolve anchor timestamp
        # -------------------------------------------------
        if anchor_ts is None:
            candidates = []

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
                raise ValueError("Cannot infer anchor_ts from data handlers")

            anchor_ts = min(candidates)

        self._anchor_ts = anchor_ts

        if self.mode == EngineMode.BACKTEST and not hasattr(self, "_history_start_ts"):
            raise RuntimeError(
                "BACKTEST warmup requires load_history() to be called first"
            )

        log_debug(self._logger, "StrategyEngine resolved anchor_ts", anchor_ts=anchor_ts)

        # -------------------------------------------------
        # 2. Warm up data handlers to anchor_ts
        # -------------------------------------------------
        for hmap in (
            self.ohlcv_handlers,
            self.orderbook_handlers,
            self.option_chain_handlers,
            self.iv_surface_handlers,
            self.sentiment_handlers,
        ):
            for h in hmap.values():
                if hasattr(h, "warmup_to"):
                    h.warmup_to(anchor_ts)

        # -------------------------------------------------
        # 3. Warm up feature extractor caches
        # -------------------------------------------------
        for _ in range(warmup_steps):
            self.feature_extractor.update(timestamp=anchor_ts)

        log_debug(self._logger, "StrategyEngine warmup completed")

    # -------------------------------------------------
    # Single event loop step (1 tick)
    # -------------------------------------------------
    def step(self) -> dict[str, Any]:
        """Run 1 iteration of the strategy pipeline.

        Returns a dict containing:
        - timestamp
        - context (features/models/portfolio)
        - decision score
        - target position
        - fills
        """

        if not hasattr(self, "_anchor_ts"):
            raise RuntimeError(
                "StrategyEngine.step() called before warmup(). "
                "Call engine.warmup(...) first."
            )
        if self.mode == EngineMode.BACKTEST and not hasattr(self, "_history_start_ts"):
            raise RuntimeError("BACKTEST step() called before load_history()")

        log_debug(self._logger, "StrategyEngine step() called")

        # -------------------------------------------------
        # 1. Pull current market snapshot (primary clock source)
        # -------------------------------------------------
        primary_handler = self._get_primary_ohlcv_handler()
        market_data: Any = None
        timestamp: float | None = None

        if primary_handler is not None and hasattr(primary_handler, "get_snapshot"):
            market_data = primary_handler.get_snapshot()
            if isinstance(market_data, dict):
                # tolerate different snapshot key conventions
                ts_val = market_data.get("timestamp")
                if ts_val is None:
                    ts_val = market_data.get("ts")
                if ts_val is not None:
                    try:
                        timestamp = float(ts_val)
                    except Exception:
                        timestamp = None

        if timestamp is None and primary_handler is not None and hasattr(primary_handler, "last_timestamp"):
            timestamp = primary_handler.last_timestamp()

        # If handler has no timestamp yet, fall back to anchor_ts (warmup resolved it)
        if timestamp is None:
            timestamp = getattr(self, "_anchor_ts", None)

        if timestamp is None:
            raise RuntimeError("Cannot resolve timestamp for step()")

        log_debug(self._logger, "StrategyEngine resolved timestamp", timestamp=timestamp)

        # -------------------------------------------------
        # 2. Feature computation (v4 snapshot-based)
        # -------------------------------------------------
        features = self.feature_extractor.update(timestamp=timestamp)
        log_debug(self._logger, "StrategyEngine computed features",
                  feature_keys=list(features.keys()))

        # Use ALL features — model decides what to use (v4 contract)
        filtered_features = features

        # -------------------------------------------------
        # 3. Model predictions
        # -------------------------------------------------
        model_outputs = {}
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
            "features": filtered_features,
            "models": model_outputs,
            "portfolio": portfolio_state_dict,
        }

        # DecisionProto.decide(context) → score
        decision_score = self.decision.decide(context)
        log_debug(self._logger, "StrategyEngine decision score", score=decision_score)

        # -------------------------------------------------
        # 5. Risk: convert score to target position
        # -------------------------------------------------
        # risk.adjust(size, features)
        size_intent = decision_score
        target_position = self.risk_manager.adjust(size_intent, context)
        log_debug(self._logger, "StrategyEngine risk target", target_position=target_position)

        # -------------------------------------------------
        # 6. Execution Pipeline
        # -------------------------------------------------

        # Execution engine is responsible for mode-specific behavior (mock/backtest/live)
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