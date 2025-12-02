from typing import Protocol, Dict

class ModelProto(Protocol):
    def predict(self, features: Dict[str, float]) -> float:
        ...