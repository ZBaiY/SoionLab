import json
from pathlib import Path

class SentimentLoader:
    """
    Load sentiment data (news, tweets) from jsonl files.
    """
    def __init__(self, symbol, root="data/sentiment/"):
        self.symbol = symbol
        self.root = Path(root)

    def load_daily(self, date: str):
        """
        Return list of text entries for a given date.
        """
        path = self.root / "twitter" / f"{date}.jsonl"
        if not path.exists():
            return []

        with open(path) as f:
            return [json.loads(line)["text"] for line in f]