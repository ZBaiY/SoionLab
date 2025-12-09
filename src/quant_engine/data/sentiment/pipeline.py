class SentimentPipeline:
    """
    Combines multiple sentiment models into one unified signal.
    """
    def __init__(self, loader, models):
        self.loader = loader
        self.models = models

    def compute(self, date: str):
        texts = self.loader.load_daily(date)
        if not texts:
            return {"sentiment": 0.0}

        combined = sum(m.score(texts) for m in self.models)
        return {"sentiment": combined}