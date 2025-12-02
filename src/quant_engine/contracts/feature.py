from typing import Protocol, Dict
import pandas as pd

class FeatureChannel(Protocol):
    def compute(self, data: pd.DataFrame) -> Dict[str, float]:
        ...