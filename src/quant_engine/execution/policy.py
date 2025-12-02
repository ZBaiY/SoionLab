from ..contracts.execution import Order

class ImmediatePolicy:
    def generate_orders(self, target_pos, current_pos):
        diff = target_pos - current_pos
        return [Order("BUY" if diff > 0 else "SELL", abs(diff))]