# risk/rules_constraints.py
"""Cash/position constraints with conservative affordability guards."""

from decimal import Decimal, ROUND_FLOOR
from typing import Any, Dict
from quant_engine.contracts.risk import RiskBase
from quant_engine.risk.registry import register_risk
from quant_engine.utils.logger import get_logger, log_debug, log_info, log_warn

# Default float tolerance for rounding errors
DEFAULT_EPS = 1e-9


@register_risk("CASH-POSITION-CONSTRAINT")
class CashPositionConstraintRule(RiskBase):
    """Cash/position constraints with floor/clip policies and min trade filters."""

    required_feature_types: set[str] = set()
    PRIORITY = 90

    def __init__(self, symbol: str, **kwargs):
        super().__init__(symbol=symbol, **kwargs)

        ignored_params = []
        for key in ("step_size", "qty_step", "qty_mode", "integer_only"):
            if key in kwargs:
                ignored_params.append(key)
        if ignored_params:
            log_warn(
                get_logger(__name__),
                "risk.cfg.ignored_portfolio_owned_param",
                symbol=symbol,
                params=ignored_params,
                reason="Portfolio-owned quantity grid params must come from portfolio snapshot",
            )

        # Fee and slippage parameters (risk-owned)
        self.fee_rate = float(kwargs.get("fee_rate", 0.001))  # 0.1% default
        self.slippage_bound_bps = float(kwargs.get("slippage_bound_bps", 10))  # 10 bps = 0.1%

        # Minimum trade filter (risk-owned)
        self.min_qty = float(kwargs.get("min_qty", 0))
        self.min_notional = float(kwargs.get("min_notional", 0))

        # Quantity handling (portfolio-owned; resolved from snapshot)
        self.eps = float(kwargs.get("eps", DEFAULT_EPS))

        self._logger = get_logger(__name__)
        log_info(
            self._logger,
            "risk.cfg.resolved_params",
            symbol=self.symbol,
            qty_step_source="portfolio",
            slippage_bound_bps=self.slippage_bound_bps,
            slippage_bound_bps_source="risk",
            fee_rate=self.fee_rate,
            fee_rate_source="risk",
            min_notional_risk=self.min_notional,
            min_notional_risk_source="risk",
            min_notional_portfolio_source="portfolio",
        )

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
        portfolio_min_qty = portfolio.get("min_qty")
        if portfolio_min_qty is not None:
            try:
                self.min_qty = float(portfolio_min_qty)
            except (TypeError, ValueError):
                pass
        portfolio_min_notional = portfolio.get("min_notional")
        if portfolio_min_notional is not None:
            try:
                self.min_notional = float(portfolio_min_notional)
            except (TypeError, ValueError):
                pass
        qty_step = Decimal(str(portfolio.get("qty_step", portfolio.get("step_size", "1"))))
        if qty_step <= 0:
            qty_step = Decimal("1")
        current_lots = portfolio.get("position_lots")
        if current_lots is None:
            current_lots = int((Decimal(str(current_position_qty)) / qty_step).to_integral_value(rounding=ROUND_FLOOR))
        current_lots = int(current_lots)

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
            qty_step=str(qty_step),
            qty_mode=portfolio.get("qty_mode", "LOTS"),
        )

        desired_lots = int((Decimal(str(desired_qty)) / qty_step).to_integral_value(rounding=ROUND_FLOOR))
        position_delta_lots = desired_lots - current_lots

        if position_delta_lots == 0:
            risk_state["constrained_target_position"] = target_fraction
            return target_fraction

        # -------------------------------------------------------
        # BUY: clip to affordability in lots
        # -------------------------------------------------------
        if position_delta_lots > 0:
            buy_lots = position_delta_lots

            # Conservative effective price with slippage bound
            slippage_factor = 1.0 + self.slippage_bound_bps / 10000.0
            p_eff = price * slippage_factor
            fee_multiplier = 1.0 + self.fee_rate

            # Max affordable quantity
            per_lot_cost = p_eff * fee_multiplier * float(qty_step)
            max_affordable_lots = int((cash / per_lot_cost)) if per_lot_cost > 0 else 0
            if max_affordable_lots <= 0:
                log_info(
                    self._logger,
                    "risk.order.clip_cash",
                    symbol=self.symbol,
                    original_buy_lots=buy_lots,
                    clipped_lots=0,
                    reason="Clipped BUY lots to 0 due to insufficient cash"
                )
                risk_state["constrained_target_position"] = current_position_frac
                return current_position_frac

            clipped_lots = min(buy_lots, max_affordable_lots)
            if clipped_lots < buy_lots:
                log_info(
                    self._logger,
                    "risk.order.clipped_cash",
                    symbol=self.symbol,
                    original_buy_lots=buy_lots,
                    clipped_lots=clipped_lots,
                    cash_available=cash,
                    p_eff=p_eff
                )

            final_buy_qty = float(Decimal(clipped_lots) * qty_step)

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

            target_lots = current_lots + clipped_lots

        # -------------------------------------------------------
        # SELL: clip to position
        # -------------------------------------------------------
        elif position_delta_lots < 0:
            sell_lots = abs(position_delta_lots)
            clipped_lots = min(sell_lots, current_lots)
            if clipped_lots <= 0:
                log_info(
                    self._logger,
                    "risk.order.drop_zero_sell",
                    symbol=self.symbol,
                    reason="Clipped SELL lots to 0, dropping order"
                )
                risk_state["constrained_target_position"] = current_position_frac
                return current_position_frac
            if clipped_lots < sell_lots:
                log_info(
                    self._logger,
                    "risk.order.clipped_position",
                    symbol=self.symbol,
                    original_sell_lots=sell_lots,
                    clipped_lots=clipped_lots,
                    position_available=current_lots
                )

            # Apply minimum trade filter
            final_sell_qty = float(Decimal(clipped_lots) * qty_step)
            notional = final_sell_qty * price
            if final_sell_qty < self.min_qty or notional < self.min_notional:
                log_info(
                    self._logger,
                    "risk.order.drop_min_trade",
                    symbol=self.symbol,
                    side="SELL",
                    qty=final_sell_qty,
                    notional=notional,
                    min_qty=self.min_qty,
                    min_notional=self.min_notional,
                    reason="SELL order below minimum thresholds"
                )
                risk_state["constrained_target_position"] = current_position_frac
                return current_position_frac

            target_lots = current_lots - clipped_lots

        # Ensure finite final target
        target_qty = float(Decimal(target_lots) * qty_step)
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
            bid = orderbook.get_attr("best_bid")
            ask = orderbook.get_attr("best_ask")

            if bid is not None and ask is not None:
                try:
                    price = (float(bid) + float(ask)) / 2.0
                    data_ts = getattr(orderbook, "data_ts", None)
                    return price, "orderbook.mid", data_ts
                except (TypeError, ValueError):
                    pass

        # Fallback to OHLCV close
        ohlcv = primary_snapshots.get("ohlcv")
        if ohlcv is not None:
            close = None
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
        if "integer_only" in kwargs:
            log_warn(
                get_logger(__name__),
                "risk.cfg.ignored_integer_only_fractional",
                symbol=symbol,
                reason="Fractional portfolios always use lot grids; integer_only is ignored",
            )
        kwargs.setdefault("min_qty", 0.0001)  # Allow very small fractional trades
        kwargs.setdefault("min_notional", 1.0)  # But enforce minimum notional
        super().__init__(symbol=symbol, **kwargs)
