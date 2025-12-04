# models/ml_model.py
from quant_engine.contracts.model import ModelProto
from .registry import register_model

@register_model("LINEAR")
class LinearModel(ModelProto):
    def __init__(self, weights: dict[str, float]):
        self.weights = weights

    def predict(self, features: dict):
        return sum(self.weights.get(k, 0) * features.get(k, 0) 
                   for k in self.weights)