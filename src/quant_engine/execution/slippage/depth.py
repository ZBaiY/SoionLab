from quant_engine.contracts.execution.slippage import SlippageBase
from quant_engine.contracts.execution.order import Order, OrderSide, OrderType
from .registry import register_slippage
from quant_engine.utils.logger import get_logger, log_debug


@register_slippage("DEPTH")
class DepthSlippage(SlippageBase):
    def __init__(self, symbol: str, depth_key="depth"):
        self.symbol = symbol
        self.depth_key = depth_key
        self._logger = get_logger(__name__)

    def apply(self, orders, market_data: dict[str, float]):
        log_debug(self._logger, "DepthSlippage received orders", count=len(orders))

        adjusted_orders = []
        depth = market_data[self.depth_key]   # e.g. numeric depth
        mid = market_data["mid"]

        for o in orders:
            slip_price = mid + (o.qty / (depth + 1e-8))
            log_debug(
                self._logger,
                "DepthSlippage computed adjusted price",
                side=o.side.value,
                qty=o.qty,
                depth=depth,
                mid=mid,
                adjusted_price=slip_price
            )
            # clone order with adjusted price
            new_o = Order(
                symbol=self.symbol,
                side=o.side,
                qty=o.qty,
                order_type=o.order_type,
                price=slip_price,
                timestamp=o.timestamp,
                tag=o.tag,
                extra=dict(o.extra)  # copy existing
            )
            new_o.extra["slippage"] = slip_price - (o.price if o.price is not None else mid)
            adjusted_orders.append(new_o)

        return adjusted_orders