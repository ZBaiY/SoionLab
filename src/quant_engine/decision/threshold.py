class ThresholdDecision:
    def __init__(self, threshold=0.0):
        self.threshold = threshold

    def decide(self, score):
        return 1.0 if score > self.threshold else -1.0