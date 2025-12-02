class LiveEngine:
    def loop(self, strategy, feed):
        for bar in feed:
            strategy.on_bar(bar)