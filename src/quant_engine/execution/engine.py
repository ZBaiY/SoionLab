# execution/engine.py
class ExecutionEngine:
    def __init__(self, policy, router, slippage, matcher):
        self.policy = policy
        self.router = router
        self.slippage = slippage
        self.matcher = matcher

    def execute(self, target_position, portfolio_state, market_data):
        # 1. raw orders
        orders = self.policy.generate(target_position, portfolio_state, market_data)

        # 2. routing logic
        routed = self.router.route(orders, market_data)

        fills = []
        for o in routed:
            # 3. slippage price
            exec_price = self.slippage.apply(o, market_data)

            # 4. matching engine execution
            fill = self.matcher.execute(o, {"exec_price": exec_price, **market_data})
            fills.append(fill)

        return fills