class FinBERT:
    """
    Placeholder: real FinBERT model can be plugged in later.
    """
    def score(self, texts):
        # fake placeholder
        return sum("good" in t.lower() for t in texts) - sum("bad" in t.lower() for t in texts)