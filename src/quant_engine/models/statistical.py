# models/physics.py
from quant_engine.contracts.model import ModelProto
from .registry import register_model

@register_model("OU_MODEL")
class OUModel(ModelProto):
    def __init__(self, theta, mu, sigma):
        self.theta = theta
        self.mu = mu
        self.sigma = sigma

    def predict(self, features: dict) -> float:
        x = features["price"]
        drift = self.theta * (self.mu - x)
        return float(drift)