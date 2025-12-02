class LinearSlippage:
    def apply(self, price, qty):
        return price * (1 + 0.0001 * qty)