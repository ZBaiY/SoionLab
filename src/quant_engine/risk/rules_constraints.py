# risk/rules_constraints.py
"""
Cash and Position Constraint Rules for Risk Layer.

Enforced Invariants:
- Cash constraint: ensures BUY orders don't exceed available cash
- Position constraint: ensures SELL orders don't exceed available position
- Minimum trade filter: drops orders below min_qty or min_notional thresholds

Policy Variants:
A) Standard portfolio (integer_only=True):
   - BUY: floor qty to integer; if qty <= 0 after floor -> drop ("floor+drop")
   - SELL: clip to available position (integer); if clipped qty == 0 -> drop
B) Fractional portfolio (integer_only=False):
   - BUY: clip by cash affordability; if below min thresholds -> drop
   - SELL: clip to available position (float); if below min thresholds -> drop

Slippage Handling:
- Conservative estimation using slippage_bound_bps for BUY affordability:
  p_eff = mid_price * (1 + slippage_bound_bps/10000)
  required_cash_est = qty * p_eff * (1 + fee_rate)
- Portfolio layer is final guard using ACTUAL fill price post-slippage

Configuration Parameters:
- fee_rate: estimated fee rate (default 0.001 = 0.1%)
- slippage_bound_bps: worst-case slippage in basis points (default 10 = 0.1%)
- min_qty: minimum trade quantity (default 0 = no filter)
- min_notional: minimum trade notional value (default 0 = no filter)
- integer_only: if True, floor quantities to integers (default True)
- eps: float tolerance for comparisons (default 1e-9)
"""

from typing import Any, Dict
from quant_engine.contracts.risk import RiskBase
from quant_engine.risk.registry import register_risk
from quant_engine.utils.logger import get_logger, log_debug, log_info, log_warn

# Default float tolerance for rounding errors
DEFAULT_EPS = 1e-9


@register_risk("CASH-POSITION-CONSTRAINT")
class CashPositionConstraintRule(RiskBase):
    """
    V4 cash and position constraint rule with floor+drop/clip policies.

    Standard portfolio (integer_only=True):
    - BUY: floor qty to integer; drop if qty <= 0 ("floor+drop")
    - SELL: clip to position; drop if clipped qty == 0

    Fractional portfolio (integer_only=False):
    - BUY/SELL: clip to achievable; drop if below min thresholds

    Applies minimum trade filter at risk output stage to avoid dust orders.
    Uses conservative slippage bound for BUY affordability checks.
    """

    required_feature_types: set[str] = set()

    def __init__(self, symbol: str, **kwargs):
        super().__init__(symbol=symbol, **kwargs)

        # Fee and slippage parameters
        self.fee_rate = float(kwargs.get("fee_rate", 0.001))  # 0.1% default
        self.slippage_bound_bps = float(kwargs.get("slippage_bound_bps", 10))  # 10 bps = 0.1%

        # Minimum trade filter
        self.min_qty = float(kwargs.get("min_qty", 0))
        self.min_notional = float(kwargs.get("min_notional", 0))

        # Quantity handling
        self.integer_only = bool(kwargs.get("integer_only", True))
        self.eps = float(kwargs.get("eps", DEFAULT_EPS))

        self._logger = get_logger(__name__)

    def adjust(self, size: float, context: Dict[str, Any]) -> float:
        """
        Adjust target position to respect cash/position constraints with floor+drop/clip policy.

        For STANDARD portfolio (integer_only=True):
        - BUY: floor to integer, drop if qty <= 0
        - SELL: clip to position (integer), drop if qty == 0

        For FRACTIONAL portfolio (integer_only=False):
        - BUY/SELL: clip to achievable, drop if below min thresholds

        Sizing semantics:
        - size is target allocation fraction in [0, 1]
        - risk engine maps decision score to target fraction before this rule

        Returns:
            Adjusted target position (may equal current_position if order dropped)
        """
        risk_state = context.setdefault("risk_state", {})

        # Extract portfolio state
        portfolio = context.get("portfolio", {})
        if isinstance(portfolio, dict) and "portfolio" in portfolio:
            portfolio = portfolio.get("portfolio", {})

        cash = float(portfolio.get("cash", 0.0))
        current_position_qty = float(portfolio.get("position_qty", portfolio.get("position", 0.0)))

        # Extract current price from primary snapshots
        price_info = self._get_current_price(context)
        if price_info is None:
            log_warn(
                self._logger,
                "risk.constraint.no_price",
                symbol=self.symbol,
                reason="Cannot determine current price, returning target position (no trade)"
            )
            return float(size)

        price, price_source, price_data_ts = price_info
        if price <= 0:
            log_warn(
                self._logger,
                "risk.constraint.no_price",
                symbol=self.symbol,
                reason="Cannot determine current price, returning target position (no trade)"
            )
            return float(size)

        size_float = float(size)

        log_debug(
            self._logger,
            "risk.price_trace",
            symbol=self.symbol,
            engine_ts=context.get("timestamp"),
            price_ref=price,
            price_source=price_source,
            price_data_ts=price_data_ts,
        )

        # Target sizing: interpret input size as target allocation fraction in [-1,1].
        target_fraction = max(-1.0, min(1.0, size_float))

        if target_fraction > 1.0 + self.eps:
            log_warn(
                self._logger,
                "risk.constraint.clamp_target_fraction",
                symbol=self.symbol,
                original_fraction=target_fraction,
                clamped_fraction=1.0,
                reason="Spot-only constraint; target fraction cannot exceed 1.0",
            )
            target_fraction = 1.0

        equity = cash + current_position_qty * price
        if equity <= self.eps:
            log_warn(
                self._logger,
                "risk.constraint.no_equity",
                symbol=self.symbol,
                cash=cash,
                current_position_qty=current_position_qty,
                price=price,
                equity=equity,
            )
            return 0.0

        desired_notional = target_fraction * equity
        desired_qty = desired_notional / price if price > 0 else 0.0
        target_qty = float(desired_qty)
        original_target = target_fraction
        current_position_frac = (current_position_qty * price) / equity if equity > 0 else 0.0

        risk_state["price_ref"] = price
        risk_state["price_source"] = price_source
        risk_state["price_data_ts"] = price_data_ts
        risk_state["current_qty"] = current_position_qty
        risk_state["equity"] = equity
        risk_state["current_position_frac"] = current_position_frac

        log_debug(
            self._logger,
            "risk.constraint.input",
            symbol=self.symbol,
            target_position=target_fraction,
            current_position_qty=current_position_qty,
            current_position_frac=current_position_frac,
            cash=cash,
            price_ref=price,
            equity=equity,
            desired_notional=desired_notional,
            desired_qty=desired_qty,
            integer_only=self.integer_only
        )

        # Calculate position change needed
        position_delta_qty = target_qty - current_position_qty

        if abs(position_delta_qty) < self.eps:
            risk_state["constrained_target_position"] = target_fraction
            return target_fraction

        # -------------------------------------------------------
        # BUY: apply floor+drop (standard) or clip (fractional)
        # -------------------------------------------------------
        if position_delta_qty > 0:
            buy_qty = position_delta_qty

            # Conservative effective price with slippage bound
            slippage_factor = 1.0 + self.slippage_bound_bps / 10000.0
            p_eff = price * slippage_factor
            fee_multiplier = 1.0 + self.fee_rate

            # Max affordable quantity
            max_affordable_qty = cash / (p_eff * fee_multiplier) if (p_eff * fee_multiplier) > 0 else 0.0

            if self.integer_only:
                # STANDARD: floor+drop policy
                floored_qty = float(int(buy_qty))
                if floored_qty <= 0:
                    log_info(
                        self._logger,
                        "risk.order.floor_drop",
                        symbol=self.symbol,
                        original_buy_qty=buy_qty,
                        floored_qty=0,
                        reason="BUY qty floored to 0, dropping order"
                    )
                    risk_state["constrained_target_position"] = current_position_frac
                    return current_position_frac

                # Apply cash constraint after floor
                if floored_qty > max_affordable_qty:
                    affordable_floored = float(int(max_affordable_qty))
                    if affordable_floored <= 0:
                        log_info(
                            self._logger,
                            "risk.order.floor_drop",
                            symbol=self.symbol,
                            original_buy_qty=buy_qty,
                            max_affordable=max_affordable_qty,
                            affordable_floored=0,
                            reason="Affordable qty floored to 0, dropping order"
                        )
                        risk_state["constrained_target_position"] = current_position_frac
                        return current_position_frac
                    floored_qty = affordable_floored
                    log_info(
                        self._logger,
                        "risk.order.clipped_cash",
                        symbol=self.symbol,
                        original_buy_qty=buy_qty,
                        clipped_qty=floored_qty,
                        cash_available=cash,
                        p_eff=p_eff,
                        fee_rate=self.fee_rate
                    )

                final_buy_qty = floored_qty
            else:
                # FRACTIONAL: clip policy
                clipped_qty = min(buy_qty, max_affordable_qty)
                if clipped_qty < self.eps:
                    log_info(
                        self._logger,
                        "risk.order.clip_cash",
                        symbol=self.symbol,
                        original_buy_qty=buy_qty,
                        clipped_qty=0,
                        reason="Clipped BUY qty to 0 due to insufficient cash"
                    )
                    risk_state["constrained_target_position"] = current_position_frac
                    return current_position_frac

                if clipped_qty < buy_qty:
                    log_info(
                        self._logger,
                        "risk.order.clipped_cash",
                        symbol=self.symbol,
                        original_buy_qty=buy_qty,
                        clipped_qty=clipped_qty,
                        cash_available=cash,
                        p_eff=p_eff
                    )

                final_buy_qty = clipped_qty

            # Apply minimum trade filter
            notional = final_buy_qty * price
            if final_buy_qty < self.min_qty or notional < self.min_notional:
                log_info(
                    self._logger,
                    "risk.order.drop_min_trade",
                    symbol=self.symbol,
                    side="BUY",
                    qty=final_buy_qty,
                    notional=notional,
                    min_qty=self.min_qty,
                    min_notional=self.min_notional,
                    reason="BUY order below minimum thresholds"
                )
                risk_state["constrained_target_position"] = current_position_frac
                return current_position_frac

            target_qty = current_position_qty + final_buy_qty

        # -------------------------------------------------------
        # SELL: clip to position (both standard and fractional)
        # -------------------------------------------------------
        elif position_delta_qty < 0:
            sell_qty = abs(position_delta_qty)

            # Clip to available position
            if sell_qty > current_position_qty + self.eps:
                clipped_qty = current_position_qty
                log_info(
                    self._logger,
                    "risk.order.clipped_position",
                    symbol=self.symbol,
                    original_sell_qty=sell_qty,
                    clipped_qty=clipped_qty,
                    position_available=current_position_qty
                )
            else:
                clipped_qty = sell_qty

            if self.integer_only:
                # STANDARD: floor sell qty to integer
                clipped_qty = float(int(clipped_qty))

            if clipped_qty < self.eps:
                log_info(
                    self._logger,
                    "risk.order.drop_zero_sell",
                    symbol=self.symbol,
                    reason="Clipped SELL qty to 0, dropping order"
                )
                risk_state["constrained_target_position"] = current_position_frac
                return current_position_frac

            # Apply minimum trade filter
            notional = clipped_qty * price
            if clipped_qty < self.min_qty or notional < self.min_notional:
                log_info(
                    self._logger,
                    "risk.order.drop_min_trade",
                    symbol=self.symbol,
                    side="SELL",
                    qty=clipped_qty,
                    notional=notional,
                    min_qty=self.min_qty,
                    min_notional=self.min_notional,
                    reason="SELL order below minimum thresholds"
                )
                risk_state["constrained_target_position"] = current_position_frac
                return current_position_frac

            target_qty = current_position_qty - clipped_qty

        # Ensure finite final target
        if target_qty < -1e12:
            target_qty = -1e12

        constrained_target_position = (target_qty * price) / equity if equity > 0 else 0.0
        if constrained_target_position < 0.0:
            constrained_target_position = 0.0
        if constrained_target_position > 1.0:
            constrained_target_position = 1.0
        risk_state["constrained_target_position"] = constrained_target_position

        log_debug(
            self._logger,
            "risk.constraint.output",
            symbol=self.symbol,
            original_target=original_target,
            final_target=constrained_target_position,
            position_delta_qty=target_qty - current_position_qty
        )

        return constrained_target_position

    def _get_current_price(self, context: Dict[str, Any]) -> tuple[float, str, int | None] | None:
        """Extract current price from context snapshots."""
        primary_snapshots = context.get("primary_snapshots", {})

        # Try orderbook mid price first
        orderbook = primary_snapshots.get("orderbook")
        if orderbook is not None:
            bid = None
            ask = None
            if hasattr(orderbook, "get_attr"):
                bid = orderbook.get_attr("best_bid")
                ask = orderbook.get_attr("best_ask")
            elif isinstance(orderbook, dict):
                bid = orderbook.get("best_bid")
                ask = orderbook.get("best_ask")

            if bid is not None and ask is not None:
                try:
                    price = (float(bid) + float(ask)) / 2.0
                    data_ts = getattr(orderbook, "data_ts", None)
                    return price, "orderbook.mid", data_ts
                except (TypeError, ValueError):
                    pass
            mid = None
            if isinstance(orderbook, dict):
                mid = orderbook.get("mid")
            elif hasattr(orderbook, "get_attr"):
                mid = orderbook.get_attr("mid")
            if mid is not None:
                try:
                    price = float(mid)
                    data_ts = getattr(orderbook, "data_ts", None)
                    return price, "orderbook.mid", data_ts
                except (TypeError, ValueError):
                    pass

        # Fallback to OHLCV close
        ohlcv = primary_snapshots.get("ohlcv")
        if ohlcv is not None:
            close = None
            if isinstance(ohlcv, dict):
                close = ohlcv.get("close")
            elif hasattr(ohlcv, "get_attr"):
                close = ohlcv.get_attr("close")

            if close is not None:
                try:
                    price = float(close)
                    data_ts = getattr(ohlcv, "data_ts", None)
                    return price, "ohlcv.close", data_ts
                except (TypeError, ValueError):
                    pass

        # Try market_snapshots as fallback
        market_snapshots = context.get("market_snapshots", {})
        ohlcv_map = market_snapshots.get("ohlcv", {})
        if isinstance(ohlcv_map, dict) and self.symbol in ohlcv_map:
            ohlcv = ohlcv_map[self.symbol]
            if ohlcv is not None:
                close = None
                if isinstance(ohlcv, dict):
                    close = ohlcv.get("close")
                elif hasattr(ohlcv, "get_attr"):
                    close = ohlcv.get_attr("close")
    
                if close is not None:
                    try:
                        price = float(close)
                        data_ts = getattr(ohlcv, "data_ts", None)
                        return price, "ohlcv.close", data_ts
                    except (TypeError, ValueError):
                        pass

        return None


@register_risk("FRACTIONAL-CASH-CONSTRAINT")
class FractionalCashConstraintRule(CashPositionConstraintRule):
    """
    Cash and position constraint rule for fractional portfolios.

    Same as CASH-POSITION-CONSTRAINT but with:
    - integer_only=False by default
    - Lower default min_qty for fractional trading

    Use this with FRACTIONAL portfolio type.
    """

    def __init__(self, symbol: str, **kwargs):
        # Override defaults for fractional trading
        kwargs.setdefault("integer_only", False)
        kwargs.setdefault("min_qty", 0.0001)  # Allow very small fractional trades
        kwargs.setdefault("min_notional", 1.0)  # But enforce minimum notional
        super().__init__(symbol=symbol, **kwargs)
