# execution/policy/maker_first.py
import math

from quant_engine.contracts.execution.policy import PolicyBase
from quant_engine.contracts.execution.order import (
    Order,
    OrderSide,
    OrderType,
)
from .registry import register_policy
from quant_engine.execution.utils import fee_buffer, lots_from_qty, qty_from_lots, to_decimal
from quant_engine.utils.logger import get_logger, log_debug


@register_policy("MAKER-FIRST")
class MakerFirstPolicy(PolicyBase):
    def __init__(self, symbol: str, spread_threshold=0.02):
        self.symbol = symbol
        self.spread_threshold = spread_threshold
        self._logger = get_logger(__name__)

    def generate(self, target_position, portfolio_state, market_data):
        log_debug(self._logger, "MakerFirstPolicy received target_position", target_position=target_position)
        orderbook = market_data.get("orderbook", None) if market_data else None
        
        best_bid = orderbook.get_attr("best_bid") if orderbook else None
        best_ask = orderbook.get_attr("best_ask") if orderbook else None
        if best_bid is None or best_ask is None:
            raise ValueError("MakerFirstPolicy requires bid/ask market data")
        mid = 0.5 * (best_bid + best_ask)

        cash = float(portfolio_state.get("cash", 0.0))
        step_size = to_decimal(portfolio_state.get("qty_step", portfolio_state.get("step_size", 1)))
        min_qty = float(portfolio_state.get("min_qty", 0.0))
        min_notional = float(portfolio_state.get("min_notional", 0.0))
        slippage_bps = float(portfolio_state.get("slippage_bps", 0.0))
        current_position_qty = float(
            portfolio_state.get("position_qty", portfolio_state.get("position", 0.0))
        )
        current_lots = portfolio_state.get("position_lots")
        if current_lots is None:
            current_lots = lots_from_qty(current_position_qty, step_size)
        current_lots = int(current_lots)
        equity = float(portfolio_state.get("total_equity", cash + current_position_qty * mid))
        if equity <= 0:
            return []

        desired_notional = float(target_position) * equity
        desired_qty = desired_notional / mid
        desired_lots = lots_from_qty(desired_qty, step_size)
        if desired_lots > current_lots and cash > 0.0:
            conservative_price = max(float(best_ask), float(mid))
            buffer = max(0.0, float(slippage_bps)) / 1e4
            conservative_price *= 1.0 + buffer
            per_lot_cost = conservative_price * float(step_size)
            fee_guard = fee_buffer(portfolio_state)
            if cash < per_lot_cost + fee_guard or cash < min_notional:
                return []
            if per_lot_cost <= 0:
                return []
            max_affordable_lots = int(math.floor((cash - fee_guard) / per_lot_cost))
            desired_lots = min(desired_lots, current_lots + max_affordable_lots)
        delta_lots = desired_lots - current_lots

        if delta_lots == 0:
            return []

        side = OrderSide.BUY if delta_lots > 0 else OrderSide.SELL
        qty = float(qty_from_lots(abs(delta_lots), step_size))
        notional = qty * mid
        if qty < min_qty or notional < min_notional:
            return []

        log_debug(
            self._logger,
            "MakerFirstPolicy computed diff",
            side=side.value,
            qty=qty,
            lots=abs(delta_lots),
            step_size=str(step_size),
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
