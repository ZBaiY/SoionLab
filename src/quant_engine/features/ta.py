import pandas as pd

class RSIChannel:
    def __init__(self, window=14):
        self.window = window

    def compute(self, data: pd.DataFrame):
        rsi = data["close"].diff().rolling(self.window).mean()
        return {"rsi": float(rsi.iloc[-1])}