import math

from quant_engine.contracts.execution.policy import PolicyBase
from quant_engine.contracts.execution.order import (
    Order,
    OrderSide,
    OrderType,
)
from .registry import register_policy
from quant_engine.execution.utils import (
    conservative_buy_price,
    fee_buffer,
    lots_from_qty,
    price_ref_from_market,
    qty_from_lots,
    to_decimal,
)
from quant_engine.utils.logger import get_logger, log_debug


@register_policy("TWAP")
class TWAPPolicy(PolicyBase):
    def __init__(self, symbol: str,slices=5):
        self.symbol = symbol
        self.slices = slices
        self._logger = get_logger(__name__)

    def generate(self, target_position, portfolio_state, market_data):
        log_debug(self._logger, "TWAPPolicy received target_position", target_position=target_position, slices=self.slices)
        price_ref = price_ref_from_market(market_data)
        if price_ref is None or price_ref <= 0:
            return []

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
        equity = float(portfolio_state.get("total_equity", cash + current_position_qty * price_ref))
        if equity <= 0:
            return []

        desired_notional = float(target_position) * equity
        desired_qty = desired_notional / price_ref
        desired_lots = lots_from_qty(desired_qty, step_size)
        if desired_lots > current_lots and cash > 0.0:
            conservative_price = conservative_buy_price(market_data, price_ref, slippage_bps)
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
        total_lots = abs(delta_lots)
        total_qty = float(qty_from_lots(total_lots, step_size))
        total_notional = total_qty * price_ref
        if total_qty < min_qty or total_notional < min_notional:
            return []
        slices = max(1, int(self.slices))
        lots_base = total_lots // slices
        lots_remainder = total_lots % slices
        log_debug(
            self._logger,
            "TWAPPolicy computed slice parameters",
            side=side,
            lots_total=total_lots,
            lots_each=lots_base,
            lots_remainder=lots_remainder,
            step_size=str(step_size),
            price_ref=price_ref,
            equity=equity,
            current_position_qty=current_position_qty,
            desired_qty=desired_qty,
        )

        orders = []
        for i in range(slices):
            lots = lots_base + (1 if i < lots_remainder else 0)
            if lots <= 0:
                continue
            qty_each = float(qty_from_lots(lots, step_size))
            log_debug(
                self._logger,
                "TWAPPolicy generated slice order",
                index=i,
                side=side,
                qty=qty_each,
                lots=lots,
                step_size=str(step_size),
            )
            orders.append(
                Order(
                    symbol=self.symbol,
                    side=side,
                    qty=qty_each,
                    order_type=OrderType.MARKET,
                    price=None,
                    tag=f"twap_{i}"
                )
            )
        return orders
