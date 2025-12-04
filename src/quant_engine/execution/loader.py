# execution/loader.py

from quant_engine.execution.policy.registry import build_policy
from quant_engine.execution.router.registry import build_router
from quant_engine.execution.slippage.registry import build_slippage
from quant_engine.execution.matching.registry import build_matching
from quant_engine.execution.engine import ExecutionEngine


class ExecutionLoader:
    @staticmethod
    def from_config(cfg: dict):
        """
        cfg example:
        {
            "policy":    {"type": "TWAP", "params": {"slices": 5}},
            "router":    {"type": "L1_AWARE", "params": {}},
            "slippage":  {"type": "LINEAR", "params": {"impact": 0.0005}},
            "matching":  {"type": "SIMULATED", "params": {}}
        }
        """

        # Build four components from registry
        policy = build_policy(cfg["policy"]["type"], **cfg["policy"].get("params", {}))
        router = build_router(cfg["router"]["type"], **cfg["router"].get("params", {}))
        slippage = build_slippage(cfg["slippage"]["type"], **cfg["slippage"].get("params", {}))
        matching = build_matching(cfg["matching"]["type"], **cfg["matching"].get("params", {}))

        # Combine into ExecutionEngine
        return ExecutionEngine(
            policy=policy,
            router=router,
            slippage=slippage,
            matcher=matching
        )