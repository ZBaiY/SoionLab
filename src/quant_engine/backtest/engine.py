class BacktestEngine:
    def run(self, strategy, data):
        # handler = HistoricalDataHandler("data/btc.csv")
    
        for bar in data:
            strategy.on_bar(bar)
        