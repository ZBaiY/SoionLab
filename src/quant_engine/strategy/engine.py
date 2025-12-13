from typing import Dict
from quant_engine.data.derivatives.iv.iv_handler import IVSurfaceDataHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.ohlcv.realtime import RealTimeDataHandler
from quant_engine.data.orderbook.realtime import RealTimeOrderbookHandler
from quant_engine.data.sentiment.loader import SentimentLoader
from quant_engine.features.extractor import FeatureExtractor

from quant_engine.utils.logger import get_logger, log_debug
from collections.abc import Mapping

class StrategyEngine:
    _logger = get_logger(__name__)
    """
    Orchestrator of the entire quant pipeline.
    It does not compute features or model predictions itself;
    it only coordinates each Layer.
    """

    def __init__(
        self,
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
                  model_count=len(models))

    # -------------------------------------------------
    # Single event loop step (1 tick)
    # -------------------------------------------------
    def step(self) -> Dict:
        log_debug(self._logger, "StrategyEngine step() called")
        """
        Run 1 iteration of the strategy pipeline.
        Returns a dict containing:
        - features
        - model outputs
        - decision score
        - target position
        - fills
        - portfolio snapshot
        """

        # -------------------------------------------------
        # 1. Determine current logical timestamp (ts)
        # -------------------------------------------------
        timestamp = None
        primary_handler = None

        # Prefer the OHLCV handler for the strategy's primary symbol
        for h in self.ohlcv_handlers.values():
            if getattr(h, "symbol", None) == self.symbol:
                primary_handler = h
                break

        # Fallback: any OHLCV handler
        if primary_handler is None and self.ohlcv_handlers:
            primary_handler = next(iter(self.ohlcv_handlers.values()))

        if primary_handler is not None and hasattr(primary_handler, "last_timestamp"):
            timestamp = primary_handler.last_timestamp()


        if timestamp is None:
            timestamp = 0.0

        log_debug(self._logger, "StrategyEngine resolved timestamp", timestamp=timestamp)

        # -------------------------------------------------
        # 2. Feature computation (v4 snapshot-based)
        # -------------------------------------------------
        features = self.feature_extractor.update(ts=timestamp)
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
        context = {
            "features": filtered_features,
            **model_outputs
        }

        # DecisionProto.decide(context) → score
        decision_score = self.decision.decide(context)
        log_debug(self._logger, "StrategyEngine decision score", score=decision_score)

        # -------------------------------------------------
        # 5. Risk: convert score to target position
        # -------------------------------------------------
        # risk.adjust(size, features)
        size_intent = decision_score
        target_position = self.risk_manager.adjust(size_intent, filtered_features)
        log_debug(self._logger, "StrategyEngine risk target", target_position=target_position)

        # -------------------------------------------------
        # 6. Execution Pipeline
        # -------------------------------------------------
        portfolio_state = self.portfolio.state().to_dict()

        # Use primary symbol OHLCV handler's latest tick
        market_data = None
        if primary_handler is not None and hasattr(primary_handler, "latest_tick"):
            market_data = primary_handler.get_snapshot()

        fills = self.execution_engine.execute(
            target_position=target_position,
            portfolio_state=portfolio_state,
            market_data=market_data
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
            "features": features,
            "model_outputs": model_outputs,
            "decision_score": decision_score,
            "target_position": target_position,
            "fills": fills,
            "portfolio": self.portfolio.state()
        }
        log_debug(self._logger, "StrategyEngine snapshot ready")

        return snapshot