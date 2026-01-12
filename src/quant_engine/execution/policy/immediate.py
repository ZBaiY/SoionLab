from quant_engine.contracts.execution.policy import PolicyBase
from quant_engine.contracts.execution.order import Order, OrderSide, OrderType
from .registry import register_policy
from quant_engine.utils.logger import get_logger, log_debug


@register_policy("IMMEDIATE")
class ImmediatePolicy(PolicyBase):
    def __init__(self, symbol: str):
        self.symbol = symbol
        self._logger = get_logger(__name__)

    def _get_price_ref(self, market_data):
        if not market_data:
            return None
        orderbook = market_data.get("orderbook")
        if orderbook is not None:
            bid = orderbook.get_attr("best_bid") if hasattr(orderbook, "get_attr") else None
            ask = orderbook.get_attr("best_ask") if hasattr(orderbook, "get_attr") else None
            if bid is not None and ask is not None:
                return (float(bid) + float(ask)) / 2.0
            mid = orderbook.get_attr("mid") if hasattr(orderbook, "get_attr") else None
            if mid is not None:
                return float(mid)
        ohlcv = market_data.get("ohlcv")
        if ohlcv is not None:
            close = ohlcv.get_attr("close") if hasattr(ohlcv, "get_attr") else None
            if close is not None:
                return float(close)
        return None

    def generate(self, target_position, portfolio_state, market_data):
        log_debug(self._logger, "ImmediatePolicy received target_position", target_position=target_position)
        price_ref = self._get_price_ref(market_data)
        if price_ref is None or price_ref <= 0:
            return []

        cash = float(portfolio_state.get("cash", 0.0))
        current_position_qty = float(
            portfolio_state.get("position_qty", portfolio_state.get("position", 0.0))
        )
        equity = cash + current_position_qty * price_ref
        if equity <= 0:
            return []

        desired_notional = float(target_position) * equity
        desired_qty = desired_notional / price_ref
        diff_qty = desired_qty - current_position_qty

        if abs(diff_qty) < 1e-9:
            return []

        side = "BUY" if diff_qty > 0 else "SELL"
        qty = abs(diff_qty)

        log_debug(
            self._logger,
            "ImmediatePolicy generated order",
            side=side,
            qty=qty,
            price_ref=price_ref,
            equity=equity,
            current_position_qty=current_position_qty,
            desired_qty=desired_qty,
        )

        return [
            Order(
                symbol=self.symbol,
                side=OrderSide.BUY if diff_qty > 0 else OrderSide.SELL,
                qty=qty,
                order_type=OrderType.MARKET,
                price=None,
                timestamp=None,
                tag="immediate",
                extra={
                    "time_in_force": OrderType.IOC.value
                }
            )
        ]
