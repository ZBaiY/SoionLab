import pandas as pd
from pathlib import Path

class IVLoader:
    def __init__(self, root="data/iv_surface/"):
        self.root = Path(root)

    def load_daily(self, date: str, symbol="BTC"):
        path = self.root / symbol / f"iv_{date}.parquet"
        if not path.exists():
            return None
        return pd.read_parquet(path)