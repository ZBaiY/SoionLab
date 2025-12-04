from quant_engine.features.ta import RSIChannel
from quant_engine.models.rsi import RSIModel
from quant_engine.decision.threshold import ThresholdDecision
from quant_engine.risk.atr import ATRSizer
from quant_engine.execution.policy.policy import ImmediatePolicy
from quant_engine.execution.router.router_impl import SimpleRouter
from quant_engine.execution.slippage.slippage_impl import LinearSlippage
from quant_engine.execution.matching.matching_sim import SimulatedMatchingEngine
from quant_engine.portfolio.state import PortfolioState

class Strategy:
    def __init__(self):
        self.feature = RSIChannel()
        self.model = RSIModel()
        self.decision = ThresholdDecision()
        self.risk = ATRSizer()
        self.policy = ImmediatePolicy()
        self.router = SimpleRouter()
        self.slip = LinearSlippage()
        self.match = SimulatedMatchingEngine()
        self.port = PortfolioState()

    def on_bar(self, bar):
        features = self.feature.compute(bar)
        score = self.model.predict(features)
        intent = self.decision.decide(score)
        size = self.risk.size(intent)

        orders = self.policy.generate_orders(size, self.port.position)
        routed = self.router.route(orders)

        for o in routed:
            price = self.slip.apply(bar["close"], o.qty)
            fill = self.match.fill(price, o.qty)
            self.port.position += o.qty if o.side == "BUY" else -o.qty


if __name__ == "__main__":
    import pandas as pd
    data = pd.DataFrame({"close": [100, 101, 102, 103]})
    strat = Strategy()

    for i in range(len(data)):
        strat.on_bar(data.iloc[i:i+1])