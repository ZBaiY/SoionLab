class VADER:
    def score(self, texts):
        return len([t for t in texts if "!" in t])