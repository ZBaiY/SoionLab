from quant_engine.utils.logger import (
    get_logger, log_debug, log_info, log_warn, log_error
)
from quant_engine.utils.timer import timed_block


class ExecutionEngine:
    """Execution layer: policy → router → slippage → matching.
    Does NOT touch portfolio — higher layer must handle fills."""

    def __init__(self, policy, router, slippage_model, matcher):
        self.policy = policy
        self.router = router
        self.slippage = slippage_model
        self.matcher = matcher

        self._logger = get_logger(__name__)

    def execute(self, decision: float, market_state: dict) -> list:
        """
        Single-symbol, multi-order pipeline.
        policy → router → slippage → match
        """

        # 1. Policy → list[Order]
        orders = self.policy.generate(decision, market_state)
        log_debug(self._logger, "Policy generated orders",
                  stage="policy", count=len(orders))

        if not orders:
            log_info(self._logger, "Execution skipped: no orders")
            return []

        # 2. Router → list[Order]
        routed = self.router.route(orders, market_state)
        log_debug(self._logger, "Router produced routed orders",
                  stage="router", count=len(routed))

        # 3. Slippage → list[Order]
        adjusted = self.slippage.apply(routed, market_state)
        log_debug(self._logger, "Slippage applied",
                  stage="slippage", count=len(adjusted))

        # 4. Matching → list[Fill]
        fills = self.matcher.match(adjusted, market_state)
        log_info(self._logger, "Orders matched",
                 stage="matching", fills=len(fills))

        return fills