class RSIModel:
    def __init__(self, window=14):
        self.window = window

    def predict(self, features):
        return -(features["rsi"] - 50)