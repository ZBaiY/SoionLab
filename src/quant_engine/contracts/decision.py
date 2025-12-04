from typing import Protocol

class DecisionProto(Protocol):
    def decide(self, context: dict) -> float:
        """
        ✔ 输入：context（包含各种市场数据和状态）,包括model的score, sentiment等
        context = {
            "model_score": model_score,
            "sentiment_score": sent_score,
            "regime_label": regime_label,
            "features": features,
            "raw_data": df,              # 用于 FusionDecision with FeatureExtractor
        }
        intent = decision.decide(context)
        
        ✔ 输出：调整后的信号（例如 -1 ~ 1）
        用途：你的 decision module（threshold/regime）会实现它。
        """
        ...