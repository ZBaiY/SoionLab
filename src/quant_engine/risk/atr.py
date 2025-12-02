class ATRSizer:
    def __init__(self, risk_fraction=0.02):
        self.risk_fraction = risk_fraction

    def size(self, intent, volatility=1.0):
        return intent * self.risk_fraction / volatility