# execution/policy/maker_first.py
from quant_engine.contracts.execution.policy import PolicyBase
from quant_engine.contracts.execution.order import (
    Order,
    OrderSide,
    OrderType,
)
from .registry import register_policy
from quant_engine.utils.logger import get_logger, log_debug


@register_policy("MAKER-FIRST")
class MakerFirstPolicy(PolicyBase):
    def __init__(self, symbol: str, spread_threshold=0.02):
        self.symbol = symbol
        self.spread_threshold = spread_threshold
        self._logger = get_logger(__name__)

    def generate(self, target_position, portfolio_state, market_data):
        log_debug(self._logger, "MakerFirstPolicy received target_position", target_position=target_position)
        ohlcv = market_data.get("ohlcv", None) if market_data else None
        orderbook = market_data.get("orderbook", None) if market_data else None
        
        best_bid = orderbook.get_attr("best_bid") if orderbook else None
        best_ask = orderbook.get_attr("best_ask") if orderbook else None
        if best_bid is None or best_ask is None:
            raise ValueError("MakerFirstPolicy requires bid/ask market data")
        mid = 0.5 * (best_bid + best_ask)

        cash = float(portfolio_state.get("cash", 0.0))
        current_position_qty = float(
            portfolio_state.get("position_qty", portfolio_state.get("position", 0.0))
        )
        equity = cash + current_position_qty * mid
        if equity <= 0:
            return []

        desired_notional = float(target_position) * equity
        desired_qty = desired_notional / mid
        diff_qty = desired_qty - current_position_qty

        if abs(diff_qty) < 1e-9:
            return []

        side = OrderSide.BUY if diff_qty > 0 else OrderSide.SELL
        qty = abs(diff_qty)

        log_debug(
            self._logger,
            "MakerFirstPolicy computed diff",
            side=side.value,
            qty=qty,
            price_ref=mid,
            equity=equity,
            current_position_qty=current_position_qty,
            desired_qty=desired_qty,
        )
        spread = best_ask - best_bid

        # if spread is small, use limit order (maker)
        if spread <= self.spread_threshold:
            price = best_bid if side == OrderSide.BUY else best_ask
            order_type = OrderType.LIMIT
        else:
            price = None
            order_type = OrderType.MARKET

        log_debug(self._logger, "MakerFirstPolicy generated order", side=side, qty=qty, order_type=order_type, price=price)

        return [
            Order(
                symbol=self.symbol,
                side=side,
                qty=qty,
                order_type=order_type,
                price=price,
                tag="maker_first"
            )
        ]
