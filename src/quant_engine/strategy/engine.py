# strategy/engine.py

from typing import Dict

class StrategyEngine:
    """
    Orchestrator of the entire quant pipeline.
    It does not compute features or model predictions itself;
    it only coordinates each Layer.
    """

    def __init__(
        self,
        data_handler,         # RealTimeDataHandler or HistoricalDataHandler
        feature_extractor,    # FeatureExtractor
        models,               # dict[str, ModelProto]
        decision,             # DecisionProto
        risk_manager,         # RiskProto
        execution_engine,     # ExecutionEngine
        portfolio_manager     # PortfolioManagerProto
    ):
        self.data_handler = data_handler
        self.feature_extractor = feature_extractor
        self.models = models
        self.decision = decision
        self.risk_manager = risk_manager
        self.execution_engine = execution_engine
        self.portfolio = portfolio_manager

    # -------------------------------------------------
    # Single event loop step (1 tick)
    # -------------------------------------------------
    def step(self) -> Dict:
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
        # 1. Get latest market window
        # -------------------------------------------------
        df = self.data_handler.window_df()

        # If no enough data, skip
        if df is None or len(df) == 0:
            return {"msg": "not enough data"}

        # -------------------------------------------------
        # 2. Compute features
        # -------------------------------------------------
        features = self.feature_extractor.compute(df)

        # -------------------------------------------------
        # 3. Model predictions
        # -------------------------------------------------
        model_outputs = {}
        for name, model in self.models.items():
            model_outputs[name] = model.predict(features)

        # -------------------------------------------------
        # 4. Construct decision context
        # -------------------------------------------------
        context = {
            "features": features,
            **model_outputs
        }

        # DecisionProto.decide(context) → score
        decision_score = self.decision.decide(context)

        # -------------------------------------------------
        # 5. Risk: convert score to target position
        # -------------------------------------------------
        # risk.adjust(size, features)
        size_intent = decision_score
        target_position = self.risk_manager.adjust(size_intent, features)

        # -------------------------------------------------
        # 6. Execution Pipeline
        # -------------------------------------------------
        portfolio_state = self.portfolio.state()
        market_data = self.data_handler.latest_tick()

        fills = self.execution_engine.execute(
            target_position=target_position,
            portfolio_state=portfolio_state,
            market_data=market_data
        )

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

        return snapshot