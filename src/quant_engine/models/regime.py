# models/regime.py
from quant_engine.contracts.model import ModelProto
from .registry import register_model

@register_model("VOL_REGIME")
class VolRegimeModel(ModelProto):
    def __init__(self, threshold=0.02):
        self.threshold = threshold

    def predict(self, features):
        return 1.0 if features["realized_vol"] > self.threshold else -1.0