from quant_engine.contracts.execution.slippage import SlippageModel
from quant_engine.contracts.execution.order import Order, OrderSide, OrderType
from .registry import register_slippage
from quant_engine.utils.logger import get_logger, log_debug


@register_slippage("LINEAR")
class LinearSlippage(SlippageModel):
    def __init__(self, impact=0.0005):
        self.impact = impact
        self._logger = get_logger(__name__)

    def apply(self, orders, market_data):
        log_debug(self._logger, "LinearSlippage received orders", count=len(orders))

        adjusted_orders = []
        mid = (market_data["bid"] + market_data["ask"]) / 2

        for o in orders:
            slip = self.impact * o.qty
            adjusted_price = mid + slip if o.side is OrderSide.BUY else mid - slip

            log_debug(
                self._logger,
                "LinearSlippage computed slippage",
                side=o.side.value,
                qty=o.qty,
                impact=self.impact,
                mid=mid,
                slip=slip,
                adjusted_price=adjusted_price
            )

            new_o = Order(
                side=o.side,
                qty=o.qty,
                order_type=o.order_type,
                price=adjusted_price,
                timestamp=o.timestamp,
                tag=o.tag,
                extra=dict(o.extra)
            )
            new_o.extra["slippage"] = slip
            adjusted_orders.append(new_o)

        return adjusted_orders