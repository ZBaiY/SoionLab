from typing import Protocol, Dict
import pandas as pd

class FeatureChannel(Protocol):
    def compute(self, data: pd.DataFrame) -> Dict[str, float]:
        """
        ✔ 输入：dataframe
        ✔ 输出：一个 feature dict，如 {“rsi”: 52.3, “vol”: 0.015}
        用途：features/ta.py、volatility.py、iv.py 都会实现。
        """
        ...