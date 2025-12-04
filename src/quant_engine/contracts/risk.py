from typing import Protocol

class RiskProto(Protocol):
    def adjust(self, size: float, features: dict) -> float:
        '''
        ✔ 输入：intent（决策后的目标信号），以及 volatility（如 ATR）
        ✔ 输出：头寸规模（float）
        用途：atr.py、vol_sizer.py 等。
        '''
        ...