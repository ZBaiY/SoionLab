from typing import Protocol, Dict

class ModelProto(Protocol):
    def predict(self, features: dict) -> float:
        """
        ✔ 输入：你的 feature dict
        ✔ 输出：model signal（float）
        用途：rsi.py、macd.py、ml_model.py 都会自动匹配。
        """
        ...