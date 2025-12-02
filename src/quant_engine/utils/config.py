from pydantic import BaseModel, Field
from typing import List, Dict


class ComponentConfig(BaseModel):
    class_name: str = Field(..., description="Name of the class to load dynamically.")
    params: Dict = Field(default_factory=dict)


class ExecutionStackConfig(BaseModel):
    policy: ComponentConfig
    router: ComponentConfig
    slippage: ComponentConfig
    matching: ComponentConfig


class FeatureConfig(ComponentConfig):
    """Identical to ComponentConfig but semantically used for features."""
    pass


class StrategyConfig(BaseModel):
    features: List[FeatureConfig]
    model: ComponentConfig
    decision: ComponentConfig
    risk: ComponentConfig
    execution: ExecutionStackConfig